package com.real.cassandra.queue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.Pelops;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.repository.PelopsPool;
import com.real.cassandra.queue.repository.QueueRepository;

/**
 * Unit tests for {@link CassQueue}.
 * 
 * @author Todd Burruss
 */
public class TestMain {
    private static Logger logger = LoggerFactory.getLogger(TestMain.class);

    private static PelopsPool queuePool;
    private static PelopsPool systemPool;
    private static QueueRepository qRep;
    private static AppProperties appProps;

    private static CassQueue cq;

    public static void main(String[] args) throws Exception {
        logger.info("setting up app properties");
        parseAppProperties();

        logger.info("setting up pelops pool");
        setupPelopsPool();

        logger.info("setting up queue");
        setupQueue();

        TestUtils testUtils = new TestUtils(cq);
        cq.setNearFifoOk(appProps.getNearFifo());

        int numPushers = appProps.getNumPushers();
        int numPoppers = appProps.getNumPoppers();
        int numToPushPerPusher = appProps.getNumMsgsPerPusher();
        int numToPopPerPopper = appProps.getNumMsgsPerPopper();
        long pushDelay = appProps.getPushDelay();
        long popDelay = appProps.getPopDelay();

        //
        // start a set of pushers and poppers
        //

        logger.info("starting pushers/poppers after 2 sec pause : " + numPushers + "/" + numPoppers);

        Thread.sleep(2000);

        Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();
        Set<PushPopAbstractBase> pusherSet =
                testUtils.startPushers(cq, "test", numPushers, numToPushPerPusher, pushDelay);
        Set<PushPopAbstractBase> popperSet =
                testUtils.startPoppers(cq, "test", numPoppers, numToPopPerPopper, popDelay, popQ);

        testUtils.monitorPushersPoppers(popQ, pusherSet, popperSet, null, null);

        shutdownQueueMgrAndPool();
    }

    // -----------------------

    private static void parseAppProperties() throws FileNotFoundException, IOException {
        File appPropsFile = new File("conf/app.properties");
        Properties props = new Properties();
        props.load(new FileReader(appPropsFile));
        appProps = new AppProperties(props);

        logger.info("using hosts : " + props.getProperty("hosts"));
        logger.info("using thrift port : " + props.getProperty("thriftPort"));
    }

    private static void setupQueue() {
        cq = new CassQueue(qRep, TestUtils.QUEUE_NAME, appProps.getNumPipes(), true, false);
        if (appProps.getTruncateQueue() && !appProps.getDropKeyspace()) {
            try {
                cq.truncate();
            }
            catch (Exception e) {
                logger.error("exception while truncating queue", e);
            }
        }
    }

    private static void setupPelopsPool() throws Exception {
        // must create system pool first and initialize cassandra
        systemPool =
                TestUtils.createSystemPool(appProps.getHostArr(), appProps.getThriftPort(),
                        appProps.getUseFramedTransport());
        qRep = new QueueRepository(systemPool, appProps.getReplicationFactor(), ConsistencyLevel.QUORUM);
        qRep.initCassandra(appProps.getDropKeyspace());

        queuePool =
                TestUtils.createQueuePool(appProps.getHostArr(), appProps.getThriftPort(),
                        appProps.getUseFramedTransport(), appProps.getMinCacheConnsPerHost(),
                        appProps.getMaxConnectionsPerHost(), appProps.getTargetConnectionsPerHost(),
                        appProps.getKillNodeConnectionsOnException());
        qRep.setQueuePool(queuePool);
    }

    private static void shutdownQueueMgrAndPool() {
        Pelops.shutdown();
    }

}
