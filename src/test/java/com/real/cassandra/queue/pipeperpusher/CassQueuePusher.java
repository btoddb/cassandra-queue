package com.real.cassandra.queue.pipeperpusher;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicLong;

import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PusherImpl;
import com.real.cassandra.queue.app.EnvProperties;

public class CassQueuePusher extends PushPopAbstractBase {
    private PusherImpl pusher;
    private AtomicLong numGen;
    private PrintWriter fWriter;

    public CassQueuePusher(int pusherId, CassQueueImpl cq, AtomicLong numGen, EnvProperties envProps)
            throws IOException {
        super(envProps, EnvProperties.ENV_pushDelay);
        this.numGen = numGen;
        this.pusher = cq.createPusher();
        this.fWriter = new PrintWriter(new FileWriter(String.format("pusher.%03d", pusherId)));
    }

    @Override
    protected boolean processMsg() throws Exception {
        String msgData = String.format("%012d", numGen.incrementAndGet());
        pusher.push(msgData);
        fWriter.println(System.currentTimeMillis() + "," + msgData);
        fWriter.flush();
        return true;
    }

    @Override
    protected void shutdown() throws Exception {
        pusher.shutdown();
        fWriter.close();
    }
}
