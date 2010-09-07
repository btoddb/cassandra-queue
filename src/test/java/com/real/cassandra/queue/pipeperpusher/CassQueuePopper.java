package com.real.cassandra.queue.pipeperpusher;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Queue;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.pipeperpusher.utils.EnvProperties;

public class CassQueuePopper extends PushPopAbstractBase {
    private Queue<CassQMsg> popQ;
    private PopperImpl popper;
    private PrintWriter fWriter;

    public CassQueuePopper(int popperId, CassQueueImpl cq, EnvProperties envProps, Queue<CassQMsg> popQ)
            throws Exception {
        super(envProps, EnvProperties.ENV_popDelay);
        this.popQ = popQ;
        this.popper = cq.createPopper(true);
        this.fWriter = new PrintWriter(new FileWriter(String.format("popper.%03d", popperId)));
    }

    @Override
    protected boolean processMsg() throws Exception {
        CassQMsg qMsg = popper.pop();
        if (null != qMsg) {
            popper.commit(qMsg);
            if (null != popQ) {
                popQ.add(qMsg);
            }
            fWriter.println(qMsg.getMsgData());
            fWriter.flush();
            return true;
        }
        else {
            return false;
        }
    }

    public Queue<CassQMsg> getPopQ() {
        return popQ;
    }

    @Override
    protected void shutdown() throws Exception {
        popper.shutdown();
        fWriter.close();
    }
}
