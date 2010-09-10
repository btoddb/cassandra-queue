package com.real.cassandra.queue.pipeperpusher;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.scale7.cassandra.pelops.Pelops;

import com.real.cassandra.queue.CassQMsgFactory;
import com.real.cassandra.queue.app.CassQueueUtils;
import com.real.cassandra.queue.app.EnvProperties;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
import com.real.cassandra.queue.repository.RepositoryFactoryImpl;
import com.real.cassandra.queue.utils.MyInetAddress;

public class CassQueueTestBase {

    private static ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    protected static EnvProperties baseEnvProps;
    protected static MyInetAddress inetAddr = new MyInetAddress();
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
        Pelops.shutdown();
    }

}
