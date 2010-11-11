package com.real.cassandra.queue;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.locks.Locker;
import com.real.cassandra.queue.locks.ObjectLock;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.utils.RollingStat;

public class PopperImpl {
    private static Logger logger = LoggerFactory.getLogger(PopperImpl.class);

    private static final int PUSHER_TIMEOUT_DELAY = 10000;
    private final Object pipeWatcherMonObj = new Object();
    private CassQueueImpl cq;
    private QueueRepositoryImpl qRepos;
    private boolean shutdownInProgress = false;
    private List<PipeDescriptorImpl> pipeDescList;
    private Locker<PipeDescriptorImpl> popLocker;
    private long nextPipeCounter;
    private PipeWatcher pipeWatcher;

    private RollingStat popNotEmptyStat;
    private RollingStat popEmptyStat;
    private RollingStat popLockerStat;
    private RollingStat lockerTryCountStat;
    private boolean working = false;

    public PopperImpl(CassQueueImpl cq, QueueRepositoryImpl qRepos, Locker<PipeDescriptorImpl> popLocker,
            RollingStat popNotEmptyStat, RollingStat popEmptyStat, RollingStat popLockerStat, RollingStat lockerTryCountStat) {
        this.cq = cq;
        this.qRepos = qRepos;
        this.popLocker = popLocker;
        this.popNotEmptyStat = popNotEmptyStat;
        this.popEmptyStat = popEmptyStat;
        this.popLockerStat = popLockerStat;
        this.lockerTryCountStat = lockerTryCountStat;
    }

    public void initialize(boolean startPipeWatcher) {
        if (startPipeWatcher) {
            pipeWatcher = new PipeWatcher();
            pipeWatcher.start();
        }
    }

    public CassQMsg pop() {
        long start = System.currentTimeMillis();

        if (shutdownInProgress) {
            throw new IllegalStateException("cannot pop messages when shutdown in progress");
        }

        ObjectLock<PipeDescriptorImpl> pipeLock = null;

        working = true;
        try {
            // make sure we try all pipes to get a msg. we go +1 for the case
            // where only maxPopPipeWidth = 1 and haven't loaded any pipe
            // descriptors yet
            for (int i = 0; i < cq.getMaxPopWidth() + 1; i++) {
                logger.debug("iteration {} of {}", i, cq.getMaxPopWidth() + 1);

                try {
                    pipeLock = pickAndLockPipe();

                    // if can't find a pipe to lock, try again
                    if (null == pipeLock) {
                        logger.debug("could not find a pipe to lock, trying again if retries left");
                        continue;
                    }
                    PipeDescriptorImpl pipeDesc = pipeLock.getLockedObj();
                    CassQMsg qMsg;
                    try {
                        qMsg = retrieveOldestMsgFromPipe(pipeDesc);
                    }
                    catch (Exception e) {
                        logger.error("exception while getting oldest msg from waiting pipe : {}", pipeDesc.getPipeId(),
                                e);
                        forceRefresh();
                        continue;
                    }

                    if (null != qMsg) {
                        popNotEmptyStat.addSample(System.currentTimeMillis() - start);
                        qRepos.updatePipePopCount(pipeDesc, pipeDesc.incPopCount());
                        return qMsg;
                    }

                    checkIfPipeCanBeMarkedPopFinished(pipeDesc.getPipeId());
                }
                finally {
                    if (null != pipeLock) {
                        logger.debug("releasing pipe : {}", pipeLock.getLockedObj().toString());
                        popLocker.release(pipeLock);
                    }
                }
            }

            popEmptyStat.addSample(System.currentTimeMillis() - start);
            return null;
        }
        catch (Exception e) {
            throw new CassQueueException(
                    (pipeLock != null && pipeLock.getLockedObj() != null ? "exception while pop'ing from pipeDesc : "
                            + pipeLock.getLockedObj() : "exception while selecting/locking a pipe"), e);
        }
        finally {
            working = false;
        }
    }

    /**
     * If pipe is note {@link PipeStatus#ACTIVE} or is expired then mark as
     * {@link PipeStatus#NOT_ACTIVE}. This method marks expired pipes as
     * {@link PipeStatus#NOT_ACTIVE} instead of {@link PusherImpl} to avoid race
     * condition.
     * 
     * @param pipeId
     */
    private void checkIfPipeCanBeMarkedPopFinished(UUID pipeId) {

        // it possible the cached pipe descriptor was removed by reaper, so
        // check if it's still there first
        PipeDescriptorImpl pipeDesc = qRepos.getPipeDescriptor(pipeId);

        if (pipeDesc != null) {
            // if this pipe is finished or expired, mark as pop finished
            if (!pipeDesc.isPushActive() && pipeDesc.isPopActive()) {
                // no race condition here with push status because the pusher is
                // no
                // longer active
                markPipeAsPopNotActiveAndRefresh(pipeDesc);
                logger.debug("pipe is not push active and empty, marking pop not active: {}", pipeDesc.toString());
            }
            else if (pipeTimeoutExpired(pipeDesc) && pipeDesc.isPopActive()) {

                // no race condition here because the pusher does not do
                // anything
                // with expired pipes
                qRepos.updatePipePushStatus(pipeDesc, PipeStatus.NOT_ACTIVE);

                markPipeAsPopNotActiveAndRefresh(pipeDesc);
                logger.debug("pipe has expired, marking pop not active : {}", pipeDesc.toString());
            }
            else {
                logger.debug("pipe is not ready to be removed : {}", pipeDesc.toString());
            }
        }
        else {
            logger.debug("pipe no longer exists, not checking status : {}", pipeId);
        }
    }

    private void markPipeAsPopNotActiveAndRefresh(PipeDescriptorImpl pipeDesc) {
        qRepos.updatePipePopStatus(pipeDesc, PipeStatus.NOT_ACTIVE);
        // TODO BTB:don't think we need to force wakeup just to count the stats.
        // the reaper has its own delay period
        // pipeReaper.wakeUp();

        // assuming only one pipe was removed during this refresh we can
        // perform better by reducing the counter so a "retry" occurs
        nextPipeCounter--;
        forceRefresh();
    }

    private boolean pipeTimeoutExpired(PipeDescriptorImpl pipeDesc) {
        // give the extra PUSHER_TIMEOUT_DELAY so no contention with pusher
        return System.currentTimeMillis() - pipeDesc.getStartTimestamp() - PUSHER_TIMEOUT_DELAY > cq
                .getMaxPushTimePerPipe();
    }

    public void commit(CassQMsg qMsg) {
        cq.commit(qMsg);
    }

    public CassQMsg rollback(CassQMsg qMsg) throws Exception {
        return cq.rollback(qMsg);
    }

    private void refreshPipeList() {
        synchronized (pipeWatcherMonObj) {
            pipeDescList = qRepos.getOldestPopActivePipes(cq.getName(), cq.getMaxPopWidth());
            // do not reset counter as this can cause pipes to never be queried
            // if pushes have slowed (or stopped) and a large pop width
            // nextPipeCounter = 0;
            if (logger.isDebugEnabled()) {
                for (PipeDescriptorImpl pipeDesc : pipeDescList) {
                    logger.debug(pipeDesc.toString());
                }
            }
        }
    }

    private CassQMsg retrieveOldestMsgFromPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        CassQMsg qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc);
        if (null != qMsg) {
            logger.debug("found message, moving it to 'waiting' pipe : {}", qMsg.toString());
            qRepos.moveMsgFromWaitingToPendingPipe(qMsg);
        }
        return qMsg;
    }

    private ObjectLock<PipeDescriptorImpl> pickAndLockPipe() throws Exception {
        ObjectLock<PipeDescriptorImpl> lock = null;

        if (null == pipeDescList || pipeDescList.isEmpty()) {
            logger.debug("no non-empty non-finished pipe descriptors found - signaled refresh");
            forceRefresh();
            return null;
        }

        // sync with the pipe watcher thread
        synchronized (pipeWatcherMonObj) {
            int width = pipeDescList.size() < cq.getMaxPopWidth() ? pipeDescList.size() : cq.getMaxPopWidth();
            int tryCount = 0;
            for (; tryCount < width && lock == null; tryCount++) {
                PipeDescriptorImpl cachedPipeDesc = pipeDescList.get((int) (nextPipeCounter++ % width));
                long start = System.currentTimeMillis();
                lock = popLocker.lock(cachedPipeDesc);
                popLockerStat.addSample(System.currentTimeMillis() - start);
                if (lock != null) {
                    logger.debug("locked pipe : " + cachedPipeDesc);
                    PipeDescriptorImpl pipeDesc = qRepos.getPipeDescriptor(cachedPipeDesc.getPipeId());

                    // unusual race condition, but don't want to read from
                    // cassandra unless we get the lock, but the reaper could
                    // have removed descriptor in the mean time
                    if (pipeDesc == null) {
                        logger.debug("Locked pipe, but already deleted {}", cachedPipeDesc.getPipeId());
                        popLocker.release(lock);
                        lock = null;
                    }
                }
            }

            lockerTryCountStat.addSample(tryCount);

            if (lock == null) {
                logger.debug("no pipe available for locking, num pipes tried = {}", width);
            }
        }

        return lock;
    }

    public String getQName() {
        return cq.getName();
    }

    public void shutdownAndWait() {
        shutdownInProgress = true;
        while (working) {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                // do nothing
            }
        }

        if (null != pipeWatcher) {
            pipeWatcher.shutdownAndWait();
        }
    }

    public void forceRefresh() {
        if (null != pipeWatcher) {
            pipeWatcher.wakeUp();
        }
        else {
            refreshPipeList();
        }
    }

    /**
     * Watches the list of pipe descriptors updating periodically.
     * 
     * @author Todd Burruss
     */
    class PipeWatcher implements Runnable {
        private boolean stopProcessing;
        private Thread theThread;

        public void start() {
            theThread = new Thread(this);
            theThread.setDaemon(true);
            theThread.setName(getClass().getSimpleName());
            theThread.start();

            try {
                // TODO BTB:do we need this?
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                // ignore
                Thread.interrupted();
            }
        }

        public void wakeUp() {
            theThread.interrupt();
        }

        public void shutdownAndWait() {
            stopProcessing = true;
            theThread.interrupt();
            while (theThread.isAlive()) {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    Thread.interrupted();
                    // do nothing
                }
            }
        }

        @Override
        public void run() {
            while (!stopProcessing) {
                try {
                    refreshPipeList();
                }
                catch (Exception e) {
                    logger.error("exception while refreshing pipe list", e);
                }

                try {
                    Thread.sleep(cq.getPopPipeRefreshDelay());
                }
                catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        }
    }

    public long getPopCalledCount() {
        return getPopNotEmptyCount() + getPopEmptyCount();
    }

    public long getPopNotEmptyCount() {
        return popNotEmptyStat.getTotalSamplesProcessed();
    }

    public long getPopEmptyCount() {
        return popEmptyStat.getTotalSamplesProcessed();
    }
}
