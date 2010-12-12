package com.real.cassandra.queue;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeManager;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.utils.RollingStat;

public class PopperImpl {
    private static final long TRANSACTION_GRACE_PERIOD = 2000;

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

    public CassQMsg pop() {
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
                
                logger.debug( "picked pipe = " + pd.getPipeId());

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
                    qMsg.getMsgDesc().setPopTimestamp(System.currentTimeMillis());
                    qRepos.updatePipePopCount(pd, pd.incPopCount(), qMsg.getMsgDesc());
                    return qMsg;
                }

                popEmptyStat.addSample(System.currentTimeMillis() - start);
                
                if ( !pipeMgr.checkMarkPopFinished(pd) ) {
                    return null;
                }
            }
        }
        catch (CassQueueException e) {
            throw e;
        }
        catch (Exception e) {
            throw new CassQueueException((pd != null ? "exception while pop'ing from pipeDesc : " + pd
                    : "exception while selecting/locking a pipe"), e);
        }
        finally {
            working = false;
        }
    }

    /**
     * Clear pipe manager selection so next time a pipe is needed, a new one may be selected.
     */
    public void clearPipeManagerSelection() {
        pipeMgr.clearSelection();
    }
    
    /**
     * Commit the popped message as long as the transaction timeout period has not elapsed.
     *
     * @param qMsg message to commit
     */
    public void commit(CassQMsg qMsg) {
        if ( !checkTransactionTimeout(qMsg) ) {
            cq.commit(qMsg);
        }
        else {
            throw new CassQueueException("Transaction has timed out and rolled back, cannot commit message" );
        }
    }

    /**
     * Rollback the popped message as long as the transaction timeout period has not elapsed.
     *
     * @param qMsg message to commit
     */
    public CassQMsg rollback(CassQMsg qMsg) throws Exception {
        if ( !checkTransactionTimeout(qMsg) ) {
            return cq.rollback(qMsg);
        }
        else {
            throw new CassQueueException("Transaction has timed out and already been rolled back, cannot rollback message again" );
        }
    }

    private boolean checkTransactionTimeout(CassQMsg qMsg) {
        return System.currentTimeMillis() - qMsg.getMsgDesc().getPopTimestamp() > (cq.getTransactionTimeout() + TRANSACTION_GRACE_PERIOD);
    }

    private CassQMsg retrieveOldestMsgFromPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        CassQMsg qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc);
        if (null != qMsg) {
            logger.debug("found message, moving it to 'waiting' pipe : {}", qMsg.toString());
            qRepos.moveMsgFromWaitingToPendingPipe(qMsg);
        }
        return qMsg;
    }

    private PipeDescriptorImpl pickAndLockPipe() throws Exception {
        return pipeMgr.pickPipe();
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

    public UUID getPopperId() {
        return popperId;
    }
}
