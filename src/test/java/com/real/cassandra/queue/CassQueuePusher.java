package com.real.cassandra.queue;

public class CassQueuePusher extends PushPopAbstractBase {

    public CassQueuePusher(CassQueue cq, String baseValue) {
        super(cq, baseValue);
    }

    @Override
    protected boolean processMsg(String value) throws Exception {
        cq.push(value);
        return true;
    }
}
