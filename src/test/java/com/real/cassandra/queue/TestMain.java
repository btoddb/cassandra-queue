package com.real.cassandra.queue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

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
        parseAppProperties();
        setupPelopsPool();
        setupQueueMgrAndPool();

        TestUtils testUtils = new TestUtils(cq);
        cq.setNearFifoOk(true);

        int numPushers = 4;
        int numPoppers = 4;
        int numToPushPerPusher = 1000;
        int numToPopPerPopper = 1000;
        long pushDelay = 0;
        long popDelay = 0;

        Set<CassQMsg> msgSet = new LinkedHashSet<CassQMsg>();
        Set<String> valueSet = new HashSet<String>();
        Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();

        //
        // start a set of pushers and poppers
        //

        Set<PushPopAbstractBase> pusherSet =
                testUtils.startPushers(cq, "test", numPushers, numToPushPerPusher, pushDelay);
        Set<PushPopAbstractBase> popperSet =
                testUtils.startPoppers(cq, "test", numPoppers, numToPopPerPopper, popDelay, popQ);

        //
        // process popped messages and wait until finished - make sure a message
        // is only processed once
        //

        long start = System.currentTimeMillis();
        while (!popQ.isEmpty() || !testUtils.isPushPopOpFinished(popperSet)
                || !testUtils.isPushPopOpFinished(pusherSet)) {
            CassQMsg qMsg = !popQ.isEmpty() ? popQ.remove() : null;
            if (null != qMsg) {
                if (!msgSet.add(qMsg)) {
                    fail("msg already popped - either message pushed twice or popped twice : " + qMsg.toString());
                }
                if (!valueSet.add(qMsg.getValue())) {
                    fail("value of message pushed more than once : " + qMsg.toString());
                }
            }
            else {
                testUtils.reportPopStatus(popperSet, popQ);
                try {
                    Thread.sleep(200);
                }
                catch (InterruptedException e) {
                    // do nothing
                }
            }
        }

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

    private static void fail(String msg) {
        throw new RuntimeException(msg);
    }

    private static void setupQueueMgrAndPool() {
        cq = new CassQueue(qRep, TestUtils.QUEUE_NAME, 4, true, false);
        try {
            cq.truncate();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupPelopsPool() throws Exception {
        // must create system pool first and initialize cassandra
        systemPool = TestUtils.createSystemPool(appProps.getHostArr(), appProps.getHostPort());
        qRep = new QueueRepository(systemPool, TestUtils.REPLICATION_FACTOR, TestUtils.CONSISTENCY_LEVEL);
        qRep.initCassandra(true);

        queuePool = TestUtils.createQueuePool(20, 20);
        qRep.setQueuePool(queuePool);
    }

    private static void shutdownQueueMgrAndPool() {
        Pelops.shutdown();
    }

}
