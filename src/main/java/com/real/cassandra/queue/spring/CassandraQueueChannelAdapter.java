package com.real.cassandra.queue.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueue;

/**
 * Spring channel adapter connecting a spring channel to a {@link CassQueue}.
 * 
 * @see #push(String)
 * @see #pop()
 * 
 * @author Todd Burruss
 */
public class CassandraQueueChannelAdapter {
    private static Logger logger = LoggerFactory.getLogger(CassandraQueueChannelAdapter.class);

    private CassQueue cassQueue;
    private CassandraQueueTxMgr txMgr;

    // private long checkDelay = 500; // millis

    public CassandraQueueChannelAdapter(CassQueue cassQueue) {
        this.cassQueue = cassQueue;
    }

    /**
     * Called by channel adapter periodically to grab message from queue and
     * deliver via channel.
     * 
     * @return {@link CassQMsg}
     */
    public String pop() {
        CassQMsg qMsg = null;
        try {
            qMsg = cassQueue.pop();
            if (null == qMsg) {
                return null;
            }

            // check for tx mgmt
            if (null != txMgr) {
                CassandraQueueTxMgr.setMessageOnThread(cassQueue, qMsg);
            }
            else {
                // TODO BTB:could simply tell queue to not move to delivered and
                // skip this step for optimization
                cassQueue.commit(qMsg);
            }

            return qMsg.getValue();
        }
        // propagate exception up so can be handled by tx mgmt code
        catch (Throwable e) {
            if (null == txMgr && null != qMsg) {
                try {
                    cassQueue.rollback(qMsg);
                }
                catch (Exception e1) {
                    logger.error("exception while rolling back because of a different issue", e);
                }
            }

            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            else {
                throw new RuntimeException(e);
            }
        }
    }

    public void push(String data) {
        try {
            cassQueue.push(data);
        }
        catch (Exception e) {
            logger.error("exception while pushing onto queue", e);
        }
    }

    public void setTxMgr(CassandraQueueTxMgr txMgr) {
        this.txMgr = txMgr;
    }
}
