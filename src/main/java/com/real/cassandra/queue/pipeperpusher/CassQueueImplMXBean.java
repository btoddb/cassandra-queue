package com.real.cassandra.queue.pipeperpusher;

public interface CassQueueImplMXBean {
    String JMX_MBEAN_OBJ_NAME_PREFIX = "com.real.cassq:type=Queue-";

    String getName();

    int getNumPoppers();

    int getNumPushers();

    long getMaxPushTimePerPipe();

    void setMaxPushTimePerPipe(long maxPushTimePerPipe);

    int getMaxPushesPerPipe();

    void setMaxPushesPerPipe(int maxPushesPerPipe);

    int getMaxPopWidth();

    void setMaxPopWidth(int maxPopWidth);

    long getPopCountNotEmpty();

    long getPopCountEmpty();

    double getPopAvgTime_NotEmpty();

    double getPopPerSecond_NotEmpty();

    long getPushCount();

    double getPushAvgTime();

    double getPushPerSecond();
}
