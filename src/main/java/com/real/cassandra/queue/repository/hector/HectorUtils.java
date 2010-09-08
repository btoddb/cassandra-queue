package com.real.cassandra.queue.repository.hector;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.real.cassandra.queue.app.EnvProperties;

public class HectorUtils {

    public static QueueRepositoryImpl createQueueRepository(EnvProperties envProps, ConsistencyLevel consistencyLevel) {
        return new QueueRepositoryImpl(envProps.getReplicationFactor(), consistencyLevel);
    }
}
