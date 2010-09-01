package com.real.cassandra.queue.pipeperpusher;

import com.real.cassandra.queue.CassQueueAbstractImpl;

public class CassQueueImpl extends CassQueueAbstractImpl implements CassQueueImplMXBean {
    private long maxPushTimeOfPipe;
    private int maxPushesPerPipe;

    public CassQueueImpl(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe) {
        super(qName);
        this.maxPushTimeOfPipe = maxPushTimeOfPipe;
        this.maxPushesPerPipe = maxPushesPerPipe;
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

}
