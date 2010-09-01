package com.real.cassandra.queue.repository;

import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Responsible for the raw I/O for Cassandra queues. Uses Pelops library for
 * client communication to Cassandra server.
 * 
 * <p/>
 * Requires Cassandra 0.7 or better.
 * 
 * @author Todd Burruss
 */
public abstract class QueueRepositoryAbstractImpl {
    public static final String SYSTEM_KEYSPACE_NAME = "system";
    public static final String STRATEGY_CLASS_NAME = "org.apache.cassandra.locator.SimpleStrategy";

    private final PelopsPool systemPool;
    private final int replicationFactor;
    private final ConsistencyLevel consistencyLevel;

    private PelopsPool queuePool;

    public QueueRepositoryAbstractImpl(PelopsPool systemPool, PelopsPool queuePool, int replicationFactor,
            ConsistencyLevel consistencyLevel) {
        this.systemPool = systemPool;
        this.queuePool = queuePool;
        this.replicationFactor = replicationFactor;
        this.consistencyLevel = consistencyLevel;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public PelopsPool getSystemPool() {
        return systemPool;
    }

    public PelopsPool getQueuePool() {
        return queuePool;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

}
