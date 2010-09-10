package com.real.cassandra.queue.repository.pelops;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.CachePerNodePool.Policy;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.OperandPolicy;

import com.real.cassandra.queue.app.EnvProperties;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;

public class PelopsUtils {
    // private static Logger logger =
    // LoggerFactory.getLogger(CassQueueUtils.class);

    // public static final String QUEUE_POOL_NAME = "myTestPool";
    // public static final String SYSTEM_POOL_NAME = "mySystemPool";
    // public static final String QUEUE_NAME = "myTestQueue";
    // public static final ConsistencyLevel CONSISTENCY_LEVEL =
    // ConsistencyLevel.QUORUM;

    public static PelopsPool createQueuePool(String[] hostArr, int rpcPort, boolean useFramedTransport,
            int minCachedConns, int maxConns, int targetConns, boolean killNodeConnsOnException) {
        Cluster cluster = new Cluster(hostArr, rpcPort);
        cluster.setFramedTransportRequired(useFramedTransport);

        Policy policy = new Policy();
        policy.setKillNodeConnsOnException(true);
        policy.setMaxConnectionsPerNode(maxConns);
        policy.setMinCachedConnectionsPerNode(minCachedConns);
        policy.setTargetConnectionsPerNode(targetConns);
        policy.setKillNodeConnsOnException(killNodeConnsOnException);

        OperandPolicy opPolicy = new OperandPolicy();
        opPolicy.setMaxOpRetries(10);

        PelopsPool pool = new PelopsPool();
        pool.setCluster(cluster);
        pool.setOperandPolicy(opPolicy);
        pool.setKeyspaceName(QueueRepositoryImpl.QUEUE_KEYSPACE_NAME);
        pool.setNodeDiscovery(false);
        pool.setPolicy(policy);
        pool.setPoolName(QueueRepositoryAbstractImpl.QUEUE_POOL_NAME);

        pool.initPool();
        return pool;
    }

    private static PelopsPool createSystemPool(String[] hostArr, int rpcPort, boolean useFramedTransport) {
        Cluster cluster = new Cluster(hostArr, rpcPort);
        cluster.setFramedTransportRequired(useFramedTransport);

        Policy policy = new Policy();
        policy.setKillNodeConnsOnException(true);
        policy.setMaxConnectionsPerNode(10);
        policy.setMinCachedConnectionsPerNode(1);
        policy.setTargetConnectionsPerNode(2);

        OperandPolicy opPolicy = new OperandPolicy();
        opPolicy.setMaxOpRetries(10);

        PelopsPool pool = new PelopsPool();
        pool.setCluster(cluster);
        pool.setOperandPolicy(opPolicy);
        pool.setKeyspaceName(QueueRepositoryImpl.SYSTEM_KEYSPACE_NAME);
        pool.setNodeDiscovery(false);
        pool.setPolicy(policy);
        pool.setPoolName(QueueRepositoryAbstractImpl.SYSTEM_POOL_NAME);

        pool.initPool();
        return pool;
    }

    public static QueueRepositoryImpl createQueueRepository(EnvProperties envProps, ConsistencyLevel consistencyLevel)
            throws Exception {
        // must create system pool first and initialize cassandra
        PelopsPool systemPool =
                PelopsUtils.createSystemPool(envProps.getHostArr(), envProps.getRpcPort(),
                        envProps.getUseFramedTransport());

        // pelops will produce errors after creating this pool if keyspace
        // doesn't exist
        PelopsPool queuePool =
                PelopsUtils.createQueuePool(envProps.getHostArr(), envProps.getRpcPort(),
                        envProps.getUseFramedTransport(), envProps.getMinCacheConnsPerHost(),
                        envProps.getMaxConnectionsPerHost(), envProps.getTargetConnectionsPerHost(),
                        envProps.getKillNodeConnectionsOnException());

        QueueRepositoryImpl qRepos =
                new QueueRepositoryImpl(systemPool, queuePool, envProps.getReplicationFactor(), consistencyLevel);
        qRepos.initKeyspace(envProps.getDropKeyspace());
        return qRepos;
    }
}
