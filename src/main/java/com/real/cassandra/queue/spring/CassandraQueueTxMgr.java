package com.real.cassandra.queue.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueue;

public class CassandraQueueTxMgr extends AbstractPlatformTransactionManager {
    private static final long serialVersionUID = 6652177773540264655L;

    private static Logger logger = LoggerFactory.getLogger(CassandraQueueTxMgr.class);

    private CassQueue cassQueue;

    @Override
    protected Object doGetTransaction() throws TransactionException {
        logger.debug("get tx");
        CassQueueTransactionObject txObj = new CassQueueTransactionObject();
        return txObj;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        logger.debug("tx begin");
        CassQueueTransactionObject txObject = (CassQueueTransactionObject) transaction;
        if (txObject.isTxStarted()) {
            throw new TransactionSystemException(
                    "Found existing active transaction - can only start one transaction at a time");
        }

        txObject.setTxStarted(true);
        txObject.setResourceHolder(new CassQueueResourceHolder());

        txObject.getResourceHolder().setSynchronizedWithTransaction(true);

        int timeout = determineTimeout(definition);
        if (timeout != TransactionDefinition.TIMEOUT_DEFAULT) {
            txObject.getResourceHolder().setTimeoutInSeconds(timeout);
        }
        TransactionSynchronizationManager.bindResource(getCassQueue(), txObject.getResourceHolder());
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        CassQueueTransactionObject txObj = (CassQueueTransactionObject) status.getTransaction();
        if (null == txObj) {
            throw new IllegalStateException("No transaction object found, cannot commit!");
        }

        CassQMsg qMsg = txObj.getResourceHolder().getqMsg();
        if (null != qMsg) {
            try {
                cassQueue.commit(qMsg);
            }
            catch (Exception e) {
                String msg = "exception while commiting transaction, call rollback or try again";
                logger.error(msg);
                throw new TransactionSystemException(msg, e);
            }
        }
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        CassQueueTransactionObject txObj = (CassQueueTransactionObject) status.getTransaction();
        if (null == txObj) {
            throw new IllegalStateException("No transaction object found, cannot rollback!");
        }

        CassQMsg qMsg = txObj.getResourceHolder().getqMsg();
        if (null == qMsg) {
            throw new IllegalStateException("No cassandra queue msg found, cannot rollback!");
        }

        try {
            cassQueue.rollback(qMsg);
        }
        catch (Exception e) {
            String msg = "exception while commiting transaction, call rollback or try again";
            logger.error(msg);
            throw new TransactionSystemException(msg, e);
        }
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        super.doCleanupAfterCompletion(transaction);
        // CassQueueTransactionObject txObj = (CassQueueTransactionObject)
        // transaction;
        // if (null == txObj.getResourceHolder()) {
        // throw new
        // IllegalStateException("No transaction object found, cannot rollback!");
        // }
        TransactionSynchronizationManager.unbindResource(getCassQueue());
    }

    public static void setMessageOnThread(CassQueue cq, CassQMsg qMsg) {
        CassQueueResourceHolder resHolder = (CassQueueResourceHolder) TransactionSynchronizationManager.getResource(cq);
        if (null == resHolder) {
            throw new IllegalStateException("No resource on thread.  Was transaction started?");
        }
        resHolder.setqMsg(qMsg);
    }

    public CassQueue getCassQueue() {
        return cassQueue;
    }

    public void setCassQueue(CassQueue cassQueue) {
        this.cassQueue = cassQueue;
    }

}
