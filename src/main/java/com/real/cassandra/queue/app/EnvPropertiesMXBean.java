package com.real.cassandra.queue.app;

public interface EnvPropertiesMXBean {
    String JMX_MBEAN_OBJ_NAME = "com.real.cassq:type=QueueProperties";

    String getHostArr();

    int getMaxActive();

    int getMaxIdle();

    int getRpcPort();

    int getReplicationFactor();

    int getNumPushers();

    int getNumPoppers();

    long getPushDelay();

    long getPopDelay();

    int getNumMsgs();

    // boolean isStrictFifo();

    void setNumPoppers(int value);

    void setPushDelay(long value);

    void setNumPushers(int value);

    void setPopDelay(long value);

    void setNumMsgs(int numMsgs);

}
