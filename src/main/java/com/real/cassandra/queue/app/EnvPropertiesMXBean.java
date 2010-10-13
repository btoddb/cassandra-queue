package com.real.cassandra.queue.app;

public interface EnvPropertiesMXBean {
    String JMX_MBEAN_OBJ_NAME = "com.real.cassq:type=EnvProperties";

    String[] getHostArr();

    int getRpcPort();

    int getReplicationFactor();

    int getNumPushers();

    int getNumMsgs();

    int getNumPoppers();

    long getPushDelay();

    long getPopDelay();

    // boolean isStrictFifo();

    boolean getDropKeyspace();

    boolean getTruncateQueue();

    // int getMinCacheConnsPerHost();

    int getMaxConnectionsPerHost();

    int getTargetConnectionsPerHost();

    boolean getUseFramedTransport();

    long getPipeCheckDelay();

    long getPushPipeIncrementDelay();

    void setNumPoppers(int value);

    void setPushDelay(long value);

    void setNumPushers(int value);

    void setPopDelay(long value);

}
