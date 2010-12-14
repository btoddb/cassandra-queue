package com.real.cassandra.queue;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeManager;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.utils.RollingStat;

/**
 * Clients wanting to pop messages from the queue use this class. Poppers are
 * normally created by calling {@link CassQueueImpl#createPopper()}, but can be
 * created by directly instantiating as well.
 * <p/>
 * This class is not thread safe because of optimizations surrounding locking.
 * Therefore each client (thread) should create its own instance and not share.
 * 
 * @author Todd Burruss
 */
public class PopperImpl {
    private static Logger logger = LoggerFactory.getLogger(PopperImpl.class);

    private UUID popperId;
    private CassQueueImpl cq;
    private QueueRepositoryImpl qRepos;
    private boolean shutdownInProgress = false;
    private PipeManager pipeMgr;

    private RollingStat popNotEmptyStat;
    private RollingStat popEmptyStat;
    private boolean working = false;

    public PopperImpl(UUID popperId, CassQueueImpl cq, QueueRepositoryImpl qRepos, PipeManager pipeMgr,
            RollingStat popNotEmptyStat, RollingStat popEmptyStat) {
        this.popperId = popperId;
        this.cq = cq;
        this.qRepos = qRepos;
        this.pipeMgr = pipeMgr;
        this.popNotEmptyStat = popNotEmptyStat;
        this.popEmptyStat = popEmptyStat;
    }

    /**
     * Retrieves a single message from the queue, leaving it in "pending" state
     * until {@link #commit(CassQMsg)} or {@link #rollback(CassQMsg)} is called.
     * 
     * @return {@link CassQMsg} instance if message was retrieved, null
     *         otherwise
     * 
     * @throws CassQueueException
     *             Runtime exception for unexpected anomalies.
     */
    public CassQMsg pop() throws CassQueueException {
        long start = System.currentTimeMillis();

        if (shutdownInProgress) {
            throw new IllegalStateException("cannot pop messages when shutdown in progress");
        }

        PipeDescriptorImpl pd = null;

        working = true;
        try {
            for (;;) {
                pd = pickAndLockPipe();
                if (null == pd) {
                    logger.debug("no pipes available, cannot pop");
                    return null;
                }

                logger.debug("picked pipe = " + pd.getPipeId());

                CassQMsg qMsg;
                try {
                    qMsg = retrieveOldestMsgFromPipe(pd);
                }
                catch (Exception e) {
                    logger.error("exception while getting oldest msg from waiting pipe : {}", pd.getPipeId(), e);
                    return null;
                }

                if (null != qMsg) {
                    popNotEmptyStat.addSample(System.currentTimeMillis() - start);
                    updatePoppedMsgStatus(qMsg);
                    qRepos.updatePipePopCount(pd, pd.incPopCount(), qMsg.getMsgDesc());
                    return qMsg;
                }

                popEmptyStat.addSample(System.currentTimeMillis() - start);

                if (!pipeMgr.checkMarkPopFinished(pd)) {
                    return null;
                }
            }
        }
        catch (CassQueueException e) {
            throw e;
        }
        catch (Throwable e) {
            throw new CassQueueException((pd != null ? "exception while pop'ing from pipeDesc : " + pd
                    : "exception while selecting/locking a pipe"), e);
        }
        finally {
            working = false;
        }
    }

    private void updatePoppedMsgStatus(CassQMsg qMsg) {
        logger.debug("found message, moving it to 'waiting' pipe : {}", qMsg.toString());
        qRepos.moveMsgFromWaitingToPendingPipe(qMsg);
    }

    /**
     * Clear pipe manager selection so next time a pipe is needed, a new one may
     * be selected. Intended for processing shutdown and unit testing, but
     * doesn't hurt anything except performance to call in production.
     */
    public void clearPipeManagerSelection() {
        pipeMgr.clearSelection();
    }

    /**
     * Commit the popped message as long as the transaction timeout period has
     * not elapsed.
     * 
     * @param qMsg
     *            message to commit
     * @throws CassQueueException
     *             Runtime exception for unexpected anomalies.
     */
    public void commit(CassQMsg qMsg) throws CassQueueException {
        if (!cq.checkTransactionTimeoutExpired(qMsg)) {
            cq.commit(qMsg);
        }
        else {
            throw new CassQueueException("Transaction has timed out and rolled back, cannot commit message");
        }
    }

    /**
     * Rollback the popped message as long as the transaction timeout period has
     * not elapsed.
     * 
     * @param qMsg
     *            message to commit
     * @throws CassQueueException
     *             Runtime exception for unexpected anomalies.
     */
    public CassQMsg rollback(CassQMsg qMsg) throws CassQueueException {
        if (!cq.checkTransactionTimeoutExpired(qMsg)) {
            return cq.rollback(qMsg);
        }
        else {
            throw new CassQueueException(
                    "Transaction has timed out and already been rolled back, cannot rollback message again");
        }
    }

    private CassQMsg retrieveOldestMsgFromPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        CassQMsg qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc);
        return qMsg;
    }

    private PipeDescriptorImpl pickAndLockPipe() throws Exception {
        return pipeMgr.pickPipe();
    }

    /**
     * Return name of queue.
     * 
     * @return
     */
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

        // free pipe so other clients can use it immediately instead of waiting for timeout
        clearPipeManagerSelection();
    }

    /**
     * Return number of times pop has been called, regardless of returning null
     * or not.
     * 
     * @return
     */
    public long getPopCalledCount() {
        return getPopNotEmptyCount() + getPopEmptyCount();
    }

    /**
     * Return number of times pop called and returned a message.
     * 
     * @return
     */
    public long getPopNotEmptyCount() {
        return popNotEmptyStat.getTotalSamplesProcessed();
    }

    /**
     * Return number of times pop called and no message found to return.
     * 
     * @return
     */
    public long getPopEmptyCount() {
        return popEmptyStat.getTotalSamplesProcessed();
    }

    /**
     * Return the ID of this popper instance.
     * 
     * @return
     */
    public UUID getPopperId() {
        return popperId;
    }
}
