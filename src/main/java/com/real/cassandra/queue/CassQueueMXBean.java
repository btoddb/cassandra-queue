package com.real.cassandra.queue;

public interface CassQueueMXBean {
    String JMX_MBEAN_OBJ_NAME = "com.real.cassq:type=CassQueue";

    boolean isNearFifoOk();

    String getName();

    int getNumPipes();

    long getMsgCountCurrent();

    long getPushCountTotal();

    long getPopCountTotal();

    long getCommitTotal();

    long getRollbackTotal();

    double getPopsPerSecond();

    double getPopAvgTime();

    double getPushesPerSecond();

    double getPushAvgTime();

    double getRollbacksPerSecond();

    double getRollbackAvgTime();

    double getCommitsPerSecond();

    double getCommitAvgTime();

    String[] getPipeCounts();

    double getMoveToDeliveredAvgTime();

    double getWaitingMsgAvgTime();

    double getPopLockWaitAvgTime();

    long getPopStartPipe();

    long getPushStartPipe();

    double getGetNextPushPipeAvgTime();

    double getGetNextPopPipeAvgTime();
}
