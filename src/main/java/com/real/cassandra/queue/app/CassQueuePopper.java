package com.real.cassandra.queue.app;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PopperImpl;

public class CassQueuePopper extends PushPopAbstractBase {
    private static Logger logger = LoggerFactory.getLogger(CassQueuePopper.class);

    private Queue<CassQMsg> popQ;
    private PopperImpl popper;
    private PrintWriter fWriter;

    public CassQueuePopper(int popperId, CassQueueImpl cq, QueueProperties envProps, Queue<CassQMsg> popQ)
            throws Exception {
        super(envProps, QueueProperties.ENV_popDelay);
        this.popQ = popQ;
        this.popper = cq.createPopper(true);
        this.fWriter = new PrintWriter(new FileWriter(String.format("target/popper.%03d", popperId)));
    }

    @Override
    protected boolean processMsg() {
        CassQMsg qMsg = popper.pop();
        if (null != qMsg) {
            popper.commit(qMsg);
            logger.debug("commited message : {} = {}", qMsg.getMsgId(), qMsg.getMsgData());
            if (null != popQ) {
                popQ.add(qMsg);
            }
            fWriter.println(System.currentTimeMillis() + "," + qMsg.getMsgData());
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
    protected void shutdownAndWait() {
        popper.shutdownAndWait();
        fWriter.close();
    }
}
