package com.real.cassandra.queue.pipeperpusher;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.scale7.cassandra.pelops.Pelops;

import com.real.cassandra.queue.pipeperpusher.utils.EnvProperties;
import com.real.cassandra.queue.pipeperpusher.utils.CassQueueUtils;

public class PipePerPusherTestBase {

    private static ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    protected static EnvProperties baseEnvProps;
    protected static MyInetAddress inetAddr = new MyInetAddress();
    protected static QueueRepositoryImpl qRepos;
    protected static CassQMsgFactory qMsgFactory = new CassQMsgFactory();

    public PipePerPusherTestBase() {
        super();
    }

    @BeforeClass
    public static void setupTestClass() throws Exception {
        baseEnvProps = CassQueueUtils.createEnvPropertiesWithDefaults();
        CassQueueUtils.startCassandraInstance();
        qRepos = CassQueueUtils.createQueueRepository(baseEnvProps, consistencyLevel);
    }

    @AfterClass
    public static void shutdownPelopsPool() throws Exception {
        Pelops.shutdown();
    }

}
