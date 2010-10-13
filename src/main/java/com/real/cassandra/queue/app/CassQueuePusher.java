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

    public CassQueuePusher(int pusherId, CassQueueImpl cq, AtomicLong numGen, QueueProperties envProps)
            throws IOException {
        super(envProps, QueueProperties.ENV_pushDelay);
        this.numGen = numGen;
        this.pusher = cq.createPusher();
        this.fWriter = new PrintWriter(new FileWriter(String.format("pusher.%03d", pusherId)));
    }

    @Override
    protected boolean processMsg() {
        String msgData = String.format("%012d", numGen.incrementAndGet());
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
        logger.debug("pushed message : {} = {}", qMsg.getMsgId(), qMsg.getMsgData());
        fWriter.println(System.currentTimeMillis() + "," + msgData);
        fWriter.flush();
        return true;
    }

    @Override
    protected void shutdown() {
        pusher.shutdown();
        fWriter.close();
    }
}
