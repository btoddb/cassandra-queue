package com.real.cassandra.queue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.Pelops;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.repository.QueueRepository;

/**
 * Unit tests for {@link CassQueue}.
 * 
 * @author Todd Burruss
 */
public class TestMain {
    private static Logger logger = LoggerFactory.getLogger(TestMain.class);

    private static QueueRepository qRepository;
    private static EnvProperties envProps;

    private static CassQueue cq;

    public static void main(String[] args) throws Exception {
        logger.info("setting up app properties");
        parseAppProperties();

        logger.info("setting up pelops pool");
        setupQueueSystemAndPelopsPool();

        logger.info("setting up queue");
        setupQueue();

        TestUtils testUtils = new TestUtils(cq);
        cq.setNearFifoOk(envProps.getNearFifo());

        //
        // start a set of pushers and poppers
        //

        logger.info("starting pushers/poppers after 2 sec pause : " + envProps.getNumPushers() + "/"
                + envProps.getNumPoppers());

        // must wait for keyspace creation to propagate
        Thread.sleep(2000);

        Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();
        List<PushPopAbstractBase> pusherSet = testUtils.startPushers(cq, "test", envProps);
        List<PushPopAbstractBase> popperSet = testUtils.startPoppers(cq, "test", popQ, envProps);

        testUtils.monitorPushersPoppers(popQ, pusherSet, popperSet, null, null);

        shutdownQueueMgrAndPool();
    }

    // -----------------------

    private static void parseAppProperties() throws FileNotFoundException, IOException {
        File appPropsFile = new File("conf/app.properties");
        Properties props = new Properties();
        props.load(new FileReader(appPropsFile));
        envProps = new EnvProperties(props);

        logger.info("using hosts : " + props.getProperty("hosts"));
        logger.info("using thrift port : " + props.getProperty("thriftPort"));
    }

    private static void setupQueue() throws Exception {
        cq = TestUtils.setupQueue(qRepository, TestUtils.QUEUE_NAME, envProps, true, false);
    }

    private static void setupQueueSystemAndPelopsPool() throws Exception {
        qRepository = TestUtils.setupQueueSystemAndPelopsPool(envProps, ConsistencyLevel.QUORUM);
    }

    private static void shutdownQueueMgrAndPool() {
        Pelops.shutdown();
    }

}
