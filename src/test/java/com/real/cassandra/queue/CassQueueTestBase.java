package com.real.cassandra.queue;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.real.cassandra.queue.app.CassQueueUtils;
import com.real.cassandra.queue.app.QueueProperties;
import com.real.cassandra.queue.repository.HectorUtils;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;

public class CassQueueTestBase {

    protected static QueueProperties baseEnvProps;
    protected static QueueRepositoryImpl qRepos;
    protected static CassQMsgFactory qMsgFactory = new CassQMsgFactory();

    public CassQueueTestBase() {
        super();
    }

    @BeforeClass
    public static void setupTestClass() throws Exception {
        baseEnvProps = CassQueueUtils.createEnvPropertiesWithDefaults();
        CassQueueUtils.startCassandraInstance();
        qRepos = HectorUtils.createQueueRepository(baseEnvProps);
    }

    @AfterClass
    public static void shutdown() throws Exception {
        if (null != qRepos) {
            qRepos.shutdown();
        }
    }

}
