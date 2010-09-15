package com.real.cassandra.queue.pipeperpusher;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.real.cassandra.queue.CassQMsgFactory;
import com.real.cassandra.queue.app.CassQueueUtils;
import com.real.cassandra.queue.app.EnvProperties;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
import com.real.cassandra.queue.repository.RepositoryFactoryImpl;

public class CassQueueTestBase {

    private static ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    protected static EnvProperties baseEnvProps;
    protected static QueueRepositoryAbstractImpl qRepos;
    protected static CassQMsgFactory qMsgFactory = new CassQMsgFactory();

    public CassQueueTestBase() {
        super();
    }

    @BeforeClass
    public static void setupTestClass() throws Exception {
        baseEnvProps = CassQueueUtils.createEnvPropertiesWithDefaults();
        CassQueueUtils.startCassandraInstance();
        qRepos = new RepositoryFactoryImpl().createInstance(baseEnvProps, consistencyLevel);
    }

    @AfterClass
    public static void shutdownPelopsPool() throws Exception {
        qRepos.shutdown();
    }

}
