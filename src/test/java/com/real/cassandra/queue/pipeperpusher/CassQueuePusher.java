package com.real.cassandra.queue.pipeperpusher;

import com.real.cassandra.queue.EnvProperties;

public class CassQueuePusher extends PushPopAbstractBase {
    private PusherImpl pusher;

    public CassQueuePusher(CassQueueImpl cq, String baseValue, EnvProperties envProps) {
        super(baseValue, envProps, EnvProperties.ENV_pushDelay);
        pusher = cq.createPusher();
    }

    @Override
    protected boolean processMsg(String value) throws Exception {
        pusher.push(value);
        return true;
    }
}
