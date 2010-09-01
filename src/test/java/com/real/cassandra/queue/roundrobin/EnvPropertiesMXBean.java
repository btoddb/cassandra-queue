package com.real.cassandra.queue.roundrobin;

public interface EnvPropertiesMXBean {
    String JMX_MBEAN_OBJ_NAME = "com.real.cassq:type=EnvProperties";

    String[] getHostArr();

    int getThriftPort();

    int getReplicationFactor();

    int getNumPushers();

    int getNumMsgsPerPusher();

    int getNumMsgsPerPopper();

    int getNumPoppers();

    long getPushDelay();

    long getPopDelay();

    boolean getNearFifo();

    boolean getDropKeyspace();

    boolean getTruncateQueue();

    int getNumPipes();

    int getMinCacheConnsPerHost();

    int getMaxConnectionsPerHost();

    int getTargetConnectionsPerHost();

    boolean getKillNodeConnectionsOnException();

    boolean getUseFramedTransport();

    long getPipeCheckDelay();

    long getPushPipeIncrementDelay();

    void setNumPoppers(int value);

    void setPushDelay(long value);

    void setNumPushers(int value);

    void setNumMsgsPerPopper(int value);

    void setNumMsgsPerPusher(int value);

    void setPopDelay(long value);

    void setPushPipeIncrementDelay(long value);

}
