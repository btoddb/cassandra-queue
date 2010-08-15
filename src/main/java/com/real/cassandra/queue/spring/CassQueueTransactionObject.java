package com.real.cassandra.queue.spring;

public class CassQueueTransactionObject {
    private CassQueueResourceHolder resourceHolder;
    private boolean txStarted = false;

    // private CassQMsg qMsg;
    //
    // public CassQMsg getqMsg() {
    // return qMsg;
    // }
    //
    // public void setqMsg(CassQMsg qMsg) {
    // this.qMsg = qMsg;
    // }

    public CassQueueResourceHolder getResourceHolder() {
        return resourceHolder;
    }

    public void setResourceHolder(CassQueueResourceHolder resourceHolder) {
        this.resourceHolder = resourceHolder;
    }

    public boolean isTxStarted() {
        return txStarted;
    }

    public void setTxStarted(boolean txStarted) {
        this.txStarted = txStarted;
    }

}
