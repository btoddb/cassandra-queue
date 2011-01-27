package com.real.cassandra.queue.pipes;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQueueException;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.locks.Locker;
import com.real.cassandra.queue.locks.ObjectLock;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;

/**
 * Manage picking, locking, and releasing pipes as needed for a single client
 * thread. This class should be thread-safe in case it is executed in separate
 * threads of the same popper.
 * <p/>
 * Also handles marking pipes as push finished when appropriate.
 * 
 * @author Todd Burruss
 */
public class PipeManager {
    private static Logger logger = LoggerFactory.getLogger(PipeManager.class);

    public static final long GRACE_EXTRA_EXPIRE_TIME = 1000;

    private static final int MAX_LOCK_ACQUIRE_TRIES = 20;
    private static final long LOCK_ACQUIRE_RETRY_WAIT = 50; // millis
    private static final int MAX_PIPES_TO_RETRIEVE = 100;

    final private CassQueueImpl cq;
    final private QueueRepositoryImpl qRepos;
    final private Locker<QueueDescriptor> pipeCollectionLocker;
    final private UUID popperId;

    private PipeDescriptorImpl currentPipe;
    private final Object currentPipeMonitor = new Object();

    private ObjectLock<QueueDescriptor> pipeCollectionLock;
    private long maxOwnerIdleTime;
    private int maxPipesToRetrieve = MAX_PIPES_TO_RETRIEVE;

    private QueueDescriptor queueDescriptor;

    public PipeManager(QueueRepositoryImpl qRepos, CassQueueImpl cq, UUID popperId,
            Locker<QueueDescriptor> pipeCollectionLocker) {
        this.qRepos = qRepos;
        this.cq = cq;
        this.popperId = popperId;
        this.pipeCollectionLocker = pipeCollectionLocker;

        this.queueDescriptor = new QueueDescriptor(this.cq.getName());

        setMaxOwnerIdleTime(cq.getTransactionTimeout());
    }

    /**
     * Pick a pipe for the popper to use. If no pipe has been selected, a
     * "selection lock" will be acquired and a pipe will be selected. If a pipe
     * has been selected and it is still usable (not expired, not empty, not
     * closed) then it is used. If the current pipe has expired, it is released.
     * 
     * @return
     * @throws Exception
     */
    public PipeDescriptorImpl pickPipe() {
        synchronized (currentPipeMonitor) {
            if (pipeStillUsable()) {
                logger.debug("{} : pipe is still usable = {}", popperId, currentPipe);
                ownPipe(currentPipe);
                return currentPipe;
            }

            currentPipe = null;

            logger.debug("{} : picking new pipe", popperId);

            // get the lock so we can select a pipe and not conflict with other
            // poppers
            try {
                acquirePipeCollectionLock();
            }
            catch (CassQueueException e) {
                logger.info(e.getMessage());
                return null;
            }

            // iterate over available pipe descriptors looking for ownable pipes
            try {
                // if no pipes available, return now
                List<PipeDescriptorImpl> pipeDescList = retrievePipeList();
                if (null == pipeDescList || pipeDescList.isEmpty()) {
                    logger.debug("no non-empty non-finished pipe descriptors found");
                    return null;
                }

                for (PipeDescriptorImpl pd : pipeDescList) {
                    if (checkPipeOwnable(pd)) {
                        ownPipe(pd);
                        logger.debug("{} : picked pipe {}", popperId, pd.getPipeId());
                        return pd;
                    }
                }
            }
            finally {
                releasePipeCollectionLock();
            }

            return null;
        }
    }

    /**
     * Clear the currently selected pipe, if any. This will force the manager to
     * select a new pipe next time the popper needs one.
     */
    public void clearSelection() {
        synchronized (currentPipeMonitor) {
            currentPipe = null;
        }
    }

    private boolean checkPipeOwnable(PipeDescriptorImpl pd) {
        return (!checkOwned(pd) || checkExpiredPopOwner(pd, true) || checkSameOwner(pd)) && checkPopActive(pd);
    }

    private boolean checkPopActive(PipeDescriptorImpl pd) {
        return pd.isPopActive();
    }

    private boolean checkPushActive(PipeDescriptorImpl pd) {
        boolean result;
        if (!pd.isPushActive()) {
            result = false;
        }
        // give the extra grace so no contention with pusher
        else if (checkPushExpired(pd)) {
            // no race condition here because the pusher does not do
            // anything with expired pipes
            qRepos.updatePipePushStatus(pd, PipeStatus.NOT_ACTIVE);
            result = false;
        }
        else {
            result = true;
        }
        logger.debug("checkPushActive : " + pd.getId().toString() + " = " + result);
        return result;
    }

    private boolean checkPushExpired(PipeDescriptorImpl pd) {
        boolean result =
                System.currentTimeMillis() - pd.getPushStartTimestamp() > (cq.getMaxPushTimePerPipe() + GRACE_EXTRA_EXPIRE_TIME);
        logger.debug("checkPushExpired : " + pd.getId().toString() + " = " + result);
        return result;
    }

    private boolean checkSameOwner(PipeDescriptorImpl pd) {
        boolean result = popperId.equals(pd.getPopOwner());
        logger.debug("checkSameOwner : " + pd.getId().toString() + " = " + result);
        return result;
    }

    private PipeDescriptorImpl ownPipe(PipeDescriptorImpl pd) {
        pd.setPopOwner(popperId);
        pd.setPopOwnTimestamp(System.currentTimeMillis());
        qRepos.savePipePopOwner(pd, popperId, pd.getPopOwnTimestamp());
        currentPipe = pd;
        return pd;
    }

    private boolean checkOwned(PipeDescriptorImpl pd) {
        boolean result = null != pd.getPopOwner();
        logger.debug("checkOwned : " + pd.getId().toString() + " = " + result);
        return result;
    }

    private boolean checkExpiredPopOwner(PipeDescriptorImpl pd, boolean includeGrace) {
        // if grace, then add a second so as not to compete with other threads
        if (null == pd.getPopOwnTimestamp()) {
            return true;
        }

        boolean result =
                System.currentTimeMillis() - pd.getPopOwnTimestamp() > (!includeGrace ? maxOwnerIdleTime
                        : maxOwnerIdleTime + GRACE_EXTRA_EXPIRE_TIME);
        logger.debug("checkExpiredPopOwner : " + pd.getId().toString() + " = " + result);
        return result;
    }

    private void acquirePipeCollectionLock() {
        pipeCollectionLock =
                pipeCollectionLocker.lock(queueDescriptor, MAX_LOCK_ACQUIRE_TRIES, LOCK_ACQUIRE_RETRY_WAIT);
        if (null != pipeCollectionLock) {
            logger.debug("{} : got pipeCollectionLock", popperId);
        }
        else {
            throw new CassQueueException("Cannot acquire lock for picking a new pipe descriptor.  Tried "
                    + MAX_LOCK_ACQUIRE_TRIES + " times before giving up");
        }
    }

    private void releasePipeCollectionLock() {
        pipeCollectionLocker.release(pipeCollectionLock);
    }

    private boolean pipeStillUsable() {
        if (null != currentPipe) {
            return !checkExpiredPopOwner(currentPipe, false) && checkPopActive(currentPipe);
        }
        else {
            return false;
        }
    }

    private List<PipeDescriptorImpl> retrievePipeList() {
        return qRepos.getOldestPopActivePipes(cq.getName(), maxPipesToRetrieve);
    }

    public void setMaxOwnerIdleTime(long maxOwnerIdleTime) {
        if (maxOwnerIdleTime >= cq.getTransactionTimeout()) {
            this.maxOwnerIdleTime = maxOwnerIdleTime;
        }
        else {
            logger.warn("It is not allowed to set 'max owner idle time' less than queue's transaction timeout.  Will set it to transaction timeout value, "
                    + cq.getTransactionTimeout() + "ms");
            this.maxOwnerIdleTime = cq.getTransactionTimeout();
        }
    }

    public long getMaxOwnerIdleTime() {
        return maxOwnerIdleTime;
    }

    /**
     * If pipe is no longer push active it is marked as pop finished. This
     * method assumes that the pipe is "empty".
     * 
     * @param pipeDesc
     * @return true if pipe is marked finished
     */
    public boolean checkMarkPopFinished(PipeDescriptorImpl pipeDesc) {
        synchronized (currentPipeMonitor) {
            UUID pipeId = pipeDesc.getPipeId();

            // must update pipe descriptor even though cached because pusher
            // could
            // have marked as complete or reaper may have removed it
            pipeDesc = qRepos.getPipeDescriptor(pipeId);

            boolean result;
            if (pipeDesc != null) {
                // if this pipe is finished or expired, mark as pop finished
                if (!checkPushActive(pipeDesc) && pipeDesc.isPopActive()) {
                    // no race condition here with push status because the
                    // pusher is
                    // no longer active
                    qRepos.updatePipePopStatus(pipeDesc, PipeStatus.NOT_ACTIVE);
                    logger.debug("pipe is not push active and empty, marking pop not active: {}", pipeDesc.toString());
                    currentPipe = null;
                    result = true;
                }
                else {
                    logger.debug("pipe is not ready to be removed : {}", pipeDesc.toString());
                    result = false;
                }
            }
            else {
                logger.debug("pipe no longer exists, not checking status : {}", pipeId);
                result = false;
            }
            logger.debug("checkIfPipeCanBeMarkedPopFinished : " + pipeId + " = " + result);
            return result;
        }
    }

    public int getMaxPipesToRetrieve() {
        return maxPipesToRetrieve;
    }

    public void setMaxPipesToRetrieve(int maxPipesToRetrieve) {
        this.maxPipesToRetrieve = maxPipesToRetrieve;
    }

}
