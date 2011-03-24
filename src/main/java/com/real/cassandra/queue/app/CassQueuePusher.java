package com.real.cassandra.queue.app;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PusherImpl;

public class CassQueuePusher extends PushPopAbstractBase {
    private static Logger logger = LoggerFactory.getLogger(CassQueuePusher.class);

    private PusherImpl pusher;
    private AtomicLong numGen;
    private PrintWriter fWriter;
    private int pusherId;
    private String testerId;

    public CassQueuePusher(String testerId, int pusherId, CassQueueImpl cq, AtomicLong numGen, QueueProperties envProps)
            throws IOException {
        super(envProps, QueueProperties.ENV_pushDelay);
        this.numGen = numGen;
        this.pusher = cq.createPusher();
        this.pusherId = pusherId;
        this.fWriter = new PrintWriter(new FileWriter(String.format("target/pusher.%03d", pusherId)));
        this.testerId = testerId;
    }

    @Override
    protected boolean processMsg() {
        String msgData = String.format("%s+%03d+%012d", testerId, pusherId, numGen.incrementAndGet());
        try {
            // just to make sure timestamps do not cause requests to overwrite
            // data - cuz last timestamp wins in Cassandra
            Thread.sleep(1);
        }
        catch (InterruptedException e) {
            Thread.interrupted();
            // do nothing
        }
        CassQMsg qMsg = pusher.push(msgData);
        logger.debug("pushed message : {} = {}", qMsg.getMsgId(), new String(qMsg.getMsgDesc().getPayload()));
        fWriter.println(System.currentTimeMillis() + "," + msgData);
        fWriter.flush();
        return true;
    }

    @Override
    protected void shutdownAndWait() {
        pusher.shutdownAndWait();
        fWriter.close();
    }
}
