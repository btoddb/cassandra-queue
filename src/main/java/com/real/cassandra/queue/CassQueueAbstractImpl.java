package com.real.cassandra.queue;

public abstract class CassQueueAbstractImpl implements CassQueue {
    private String qName;

    public CassQueueAbstractImpl(String qName) {
        this.qName = qName;
    }

    @Override
    public String getName() {
        return qName;
    }
}
