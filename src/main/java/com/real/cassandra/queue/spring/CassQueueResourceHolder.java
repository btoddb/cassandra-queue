package com.real.cassandra.queue.spring;

import org.springframework.transaction.support.ResourceHolderSupport;

import com.real.cassandra.queue.CassQMsg;

public class CassQueueResourceHolder extends ResourceHolderSupport {
    private CassQMsg qMsg;

    public CassQMsg getqMsg() {
        return qMsg;
    }

    public void setqMsg(CassQMsg qMsg) {
        this.qMsg = qMsg;
    }
}
