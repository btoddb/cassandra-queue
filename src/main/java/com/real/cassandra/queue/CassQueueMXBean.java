package com.real.cassandra.queue;


public interface CassQueueMXBean {
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

    long getPopCountLocalNotEmpty();

    long getPopCountLocalEmpty();

    long getPopCountCluster();

    double getPopAvgTimeLocal_NotEmpty();

    double getPopPerSecondLocal_NotEmpty();

    long getPushCountLocal();

    double getPushAvgTimeLocal();

    double getPushPerSecondLocal();

    long getPushCountCluster();

    double getPopLockerAvgTime();
    
    double getLockTryAvtCount();

}
