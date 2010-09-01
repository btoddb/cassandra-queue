package com.real.cassandra.queue.pipeperpusher;

public interface CassQueueImplMXBean {

    long getMaxPushTimeOfPipe();

    void setMaxPushTimeOfPipe(long maxPushTimeOfPipe);

    int getMaxPushesPerPipe();

    void setMaxPushesPerPipe(int maxPushesPerPipe);

}
