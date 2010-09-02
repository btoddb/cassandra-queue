package com.real.cassandra.queue.pipeperpusher;

import com.real.cassandra.queue.CassQueueAbstractImpl;

public class CassQueueImpl extends CassQueueAbstractImpl implements CassQueueImplMXBean {
    private long maxPushTimeOfPipe;
    private int maxPushesPerPipe;
    private int popWidth;
    private QueueRepositoryImpl qRepos;
    private PipeDescriptorFactory pipeDescFactory;
    private PipeLockerImpl popLocker;

    public CassQueueImpl(QueueRepositoryImpl qRepos, PipeDescriptorFactory pipeDescFactory, String qName,
            long maxPushTimeOfPipe, int maxPushesPerPipe, int popWidth, PipeLockerImpl popLocker) {
        super(qName);

        this.qRepos = qRepos;
        this.pipeDescFactory = pipeDescFactory;
        this.maxPushTimeOfPipe = maxPushTimeOfPipe;
        this.maxPushesPerPipe = maxPushesPerPipe;
        this.popWidth = popWidth;
        this.popLocker = popLocker;
    }

    public PusherImpl createPusher() {
        return new PusherImpl(this, qRepos, pipeDescFactory);
    }

    public PopperImpl createPopper() throws Exception {
        return new PopperImpl(this, qRepos, popLocker);
    }

    @Override
    public void truncate() throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public long getMaxPushTimeOfPipe() {
        return maxPushTimeOfPipe;
    }

    @Override
    public void setMaxPushTimeOfPipe(long maxPushTimeOfPipe) {
        this.maxPushTimeOfPipe = maxPushTimeOfPipe;
    }

    @Override
    public int getMaxPushesPerPipe() {
        return maxPushesPerPipe;
    }

    @Override
    public void setMaxPushesPerPipe(int maxPushesPerPipe) {
        this.maxPushesPerPipe = maxPushesPerPipe;
    }

    public int getPopWidth() {
        return popWidth;
    }

    public void setPopWidth(int popWidth) {
        this.popWidth = popWidth;
    }

}
