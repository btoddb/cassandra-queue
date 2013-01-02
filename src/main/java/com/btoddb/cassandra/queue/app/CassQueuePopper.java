package com.btoddb.cassandra.queue.app;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.cassandra.queue.CassQMsg;
import com.btoddb.cassandra.queue.CassQueueImpl;
import com.btoddb.cassandra.queue.PopperImpl;

public class CassQueuePopper extends PushPopAbstractBase {
    private static Logger logger = LoggerFactory.getLogger(CassQueuePopper.class);

    private Queue<CassQMsg> popQ;
    private PopperImpl popper;
    private PrintWriter fWriter;

    public CassQueuePopper(int popperId, CassQueueImpl cq, QueueProperties envProps, Queue<CassQMsg> popQ)
            throws Exception {
        super(envProps, QueueProperties.ENV_popDelay);
        this.popQ = popQ;
        this.popper = cq.createPopper();
        this.fWriter = new PrintWriter(new FileWriter(String.format("target/popper.%03d", popperId)));
    }

    @Override
    protected boolean processMsg() {
        CassQMsg qMsg = popper.pop();
        if (null != qMsg) {
            popper.commit(qMsg);
            logger.debug("commited message : {} = {}", qMsg.getMsgId(), qMsg.getMsgDesc().getPayload());
            if (null != popQ) {
                popQ.add(qMsg);
            }
            fWriter.println(System.currentTimeMillis() + "," + new String(qMsg.getMsgDesc().getPayload()));
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
