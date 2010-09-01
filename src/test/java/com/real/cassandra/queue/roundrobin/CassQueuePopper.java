package com.real.cassandra.queue.roundrobin;

import java.util.Queue;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.roundrobin.CassQueueImpl;

public class CassQueuePopper extends PushPopAbstractBase {
    private Queue<CassQMsg> popQ;

    public CassQueuePopper(CassQueueImpl cq, String baseValue, EnvProperties envProps, Queue<CassQMsg> popQ) {
        super(cq, baseValue, envProps, EnvProperties.ENV_popDelay);
        this.popQ = popQ;
    }

    @Override
    protected boolean processMsg(String value) throws Exception {
        CassQMsg qMsg = cq.pop();
        if (null != qMsg) {
            cq.commit(qMsg);
            popQ.add(qMsg);
            return true;
        }
        else {
            return false;
        }
    }

    public Queue<CassQMsg> getPopQ() {
        return popQ;
    }
}
