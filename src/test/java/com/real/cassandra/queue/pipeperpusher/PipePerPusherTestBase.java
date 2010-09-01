package com.real.cassandra.queue.pipeperpusher;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.BeforeClass;

import com.real.cassandra.queue.EnvProperties;

public class PipePerPusherTestBase {

    private static ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    private static EnvProperties baseEnvProps;

    protected static MyInetAddress inetAddr = new MyInetAddress();
    protected static QueueRepositoryImpl qRepos;
    protected static CassQMsgFactory qMsgFactory = new CassQMsgFactory();

    public PipePerPusherTestBase() {
        super();
    }

    @BeforeClass
    public static void setupTestClass() throws Exception {
        baseEnvProps = TestUtils.createEnvPropertiesWithDefaults();
        TestUtils.startCassandraInstance();
        qRepos = TestUtils.createQueueRepository(baseEnvProps, consistencyLevel);
    }

}
