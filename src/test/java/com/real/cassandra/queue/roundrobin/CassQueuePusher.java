package com.real.cassandra.queue.roundrobin;

import com.real.cassandra.queue.roundrobin.CassQueueImpl;

public class CassQueuePusher extends PushPopAbstractBase {

    public CassQueuePusher(CassQueueImpl cq, String baseValue, EnvProperties envProps) {
        super(cq, baseValue, envProps, EnvProperties.ENV_pushDelay);
    }

    @Override
    protected boolean processMsg(String value) throws Exception {
        cq.push(value);
        return true;
    }
}
