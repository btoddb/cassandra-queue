package com.real.cassandra.queue;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.real.cassandra.queue.app.CassQueueUtils;
import com.real.cassandra.queue.app.EnvProperties;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
import com.real.cassandra.queue.repository.RepositoryFactoryImpl;

public class CassQueueTestBase {

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
        qRepos = new RepositoryFactoryImpl().createInstance(baseEnvProps);
    }

    @AfterClass
    public static void shutdown() throws Exception {
        qRepos.shutdown();
    }

}
