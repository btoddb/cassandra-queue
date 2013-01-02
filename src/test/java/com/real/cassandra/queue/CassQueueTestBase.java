package com.real.cassandra.queue;

import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.testutils.EmbeddedServerHelper;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;

import com.real.cassandra.queue.app.CassQueueUtils;
import com.real.cassandra.queue.app.QueueProperties;
import com.real.cassandra.queue.repository.HectorUtils;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CassQueueTestBase {

    protected static QueueProperties baseEnvProps;
    protected static QueueRepositoryImpl qRepos;
    protected static CassQMsgFactory qMsgFactory = new CassQMsgFactory();

    private static Logger log = LoggerFactory.getLogger(CassQueueTestBase.class);
    private static EmbeddedServerHelper embedded;

    protected HConnectionManager connectionManager;
    protected CassandraHostConfigurator cassandraHostConfigurator;
    protected String clusterName = "TestCluster";


    public CassQueueTestBase() {
        super();
    }

    @BeforeClass
    public static void setupTestClass() throws TTransportException, IOException, InterruptedException, ConfigurationException {
        if ( null == embedded) {
            embedded = new EmbeddedServerHelper();
            embedded.setup();
        }

        if (null == qRepos) {
            baseEnvProps = CassQueueUtils.createEnvPropertiesWithDefaults();
            qRepos = HectorUtils.createQueueRepository(baseEnvProps);
        }
    }

//    @AfterClass
//    public static void shutdown() throws Exception {
//        if (null != qRepos) {
//            qRepos.shutdown();
//        }
//    }

//    @AfterClass
//    public static void teardownCassandra() throws IOException {
//        EmbeddedServerHelper.teardown();
//        embedded = null;
//    }


    protected void setupClient() {
        cassandraHostConfigurator = new CassandraHostConfigurator("127.0.0.1:9170");
        connectionManager = new HConnectionManager(clusterName,cassandraHostConfigurator);
    }

    protected CassandraHostConfigurator getCHCForTest() {
        CassandraHostConfigurator chc = new CassandraHostConfigurator("127.0.0.1:9170");
        chc.setMaxActive(2);
        return chc;
    }

}
