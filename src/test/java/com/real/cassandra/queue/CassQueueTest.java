package com.real.cassandra.queue;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.cassandra.pelops.Pelops;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.repository.QueueRepository;

/**
 * Unit tests for {@link CassQueue}.
 * 
 * @author Todd Burruss
 */
public class CassQueueTest {
    private static Logger logger = LoggerFactory.getLogger(CassQueueTest.class);

    private static QueueRepository qRep;
    private static EmbeddedCassandraService cassandra;
    private static EnvProperties baseEnvProps;

    private CassQueue cq;
    private TestUtils testUtils;

    // @Test
    // public void testDirectColumnAccess() throws Exception {
    // String myColFam = "myColFam";
    // KeyspaceManager ksMgr =
    // Pelops.createKeyspaceManager(systemPool.getCluster());
    // ksMgr.dropKeyspace(QueueRepository.QUEUE_KEYSPACE_NAME);
    //
    // ArrayList<CfDef> cfDefList = new ArrayList<CfDef>(1);
    // cfDefList.add(new CfDef(QueueRepository.QUEUE_KEYSPACE_NAME,
    // myColFam).setComparator_type("BytesType")
    // .setKey_cache_size(0));
    //
    // KsDef ksDef = new KsDef(QueueRepository.QUEUE_KEYSPACE_NAME,
    // QueueRepository.STRATEGY_CLASS_NAME, 1, cfDefList);
    // ksMgr.addKeyspace(ksDef);
    //
    // for (int i = 0; i < 10000; i++) {
    // qRep.insert(myColFam, TestUtils.QUEUE_NAME, 0, Bytes.fromInt(i),
    // Bytes.fromInt(i));
    // }
    //
    // {
    // Selector s = Pelops.createSelector(queuePool.getPoolName());
    // SlicePredicate pred = Selector.newColumnsPredicateAll(false, 1000000);
    // int colCount =
    // s.getColumnCount(myColFam,
    // Bytes.fromUTF8(QueueRepository.formatKey(TestUtils.QUEUE_NAME, 0)),
    // pred, ConsistencyLevel.QUORUM);
    //
    // System.out.println("count = " + colCount);
    // }
    //
    // long start = System.currentTimeMillis();
    // String rowKey = QueueRepository.formatKey(TestUtils.QUEUE_NAME, 0);
    // for (int i = 0; i < 10000; i++) {
    // Selector s = Pelops.createSelector(queuePool.getPoolName());
    //
    // Column col =
    // s.getColumnFromRow(myColFam, Bytes.fromUTF8(rowKey), Bytes.fromInt(i),
    // ConsistencyLevel.QUORUM);
    //
    // // // using slicepredicate
    // // SlicePredicate pred = Selector.newColumnsPredicateAll(false, 1);
    // // List<Column> colList = s.getColumnsFromRow(myColFam, rowKey,
    // // pred, ConsistencyLevel.QUORUM);
    // // Column col = colList.get(0);
    //
    // // delete it
    // Mutator m = Pelops.createMutator(queuePool.getPoolName());
    // m.deleteColumn(myColFam, rowKey, Bytes.fromBytes(col.getName()));
    // m.execute(ConsistencyLevel.QUORUM);
    // }
    //
    // System.out.println("duration = " + (System.currentTimeMillis() - start));
    //
    // Selector s = Pelops.createSelector(queuePool.getPoolName());
    // SlicePredicate pred = Selector.newColumnsPredicateAll(false, 1000000);
    // int colCount =
    // s.getColumnCount(myColFam,
    // Bytes.fromUTF8(QueueRepository.formatKey(TestUtils.QUEUE_NAME, 0)), pred,
    // ConsistencyLevel.QUORUM);
    //
    // System.out.println("count = " + colCount);
    //
    // }

    @Test
    public void testPush() throws Exception {
        cq.setStopPipeWatcher();

        int numMsgs = 10;
        for (int i = 0; i < numMsgs; i++) {
            cq.push("xxx_" + i);
        }

        verifyWaitingQueue(numMsgs);
        verifyDeliveredQueue(0);
    }

    @Test
    public void testPop() throws Exception {
        cq.setStopPipeWatcher();

        int numMsgs = 100;
        for (int i = 0; i < numMsgs; i++) {
            cq.push("xxx_" + i);
        }

        ArrayList<CassQMsg> popList = new ArrayList<CassQMsg>(numMsgs);
        CassQMsg qMsg;
        while (0 < qRep.getCount(cq.getName())) {
            if (null != (qMsg = cq.pop())) {
                popList.add(qMsg);
            }
        }

        assertEquals("did not pop the correct amount", numMsgs, popList.size());
        for (int i = 0; i < numMsgs; i++) {
            assertEquals("events were popped out of order", "xxx_" + i, popList.get(i).getValue());
        }

        verifyWaitingQueue(0);
        verifyDeliveredQueue(numMsgs);
    }

    @Test
    public void testCommit() throws Exception {
        int numMsgs = 10;
        for (int i = 0; i < numMsgs; i++) {
            cq.push("xxx_" + i);
        }

        ArrayList<CassQMsg> popList = new ArrayList<CassQMsg>(numMsgs);
        CassQMsg evt;
        while (null != (evt = cq.pop())) {
            popList.add(evt);
        }

        for (int i = 0; i < numMsgs; i += 2) {
            cq.commit(popList.get(i));
        }

        verifyWaitingQueue(0);

        for (int i = 0; i < numMsgs; i++) {
            verifyExistsInDeliveredQueue(i, numMsgs, 0 != i % 2);
        }
    }

    @Test
    public void testRollback() throws Exception {
        int numMsgs = 10;
        for (int i = 0; i < numMsgs; i++) {
            cq.push("xxx_" + i);
        }

        ArrayList<CassQMsg> popList = new ArrayList<CassQMsg>(numMsgs);
        CassQMsg evt;
        while (null != (evt = cq.pop())) {
            popList.add(evt);
        }

        for (int i = 0; i < numMsgs; i += 2) {
            cq.rollback(popList.get(i));
        }

        for (int i = 0; i < numMsgs; i++) {
            verifyExistsInWaitingQueue(i, numMsgs, 0 == i % 2);
            verifyExistsInDeliveredQueue(i, numMsgs, 0 != i % 2);
        }
    }

    @Test
    public void testRollbackAndPopAgain() throws Exception {
        cq.setNearFifoOk(false);

        cq.push("xxx");
        cq.push("yyy");
        cq.push("zzz");

        CassQMsg evtToRollback = cq.pop();

        CassQMsg evt = cq.pop();
        assertEquals("should have popped next event", "yyy", evt.getValue());
        cq.commit(evt);

        cq.rollback(evtToRollback);

        evt = cq.pop();
        assertEquals("should have popped rolled back event again", "xxx", evt.getValue());
        cq.commit(evt);

        evt = cq.pop();
        assertEquals("should have popped last event", "zzz", evt.getValue());
        cq.commit(evt);

        assertNull("should not be anymore events", cq.pop());

        verifyDeliveredQueue(0);
        verifyWaitingQueue(0);
    }

    @Test
    public void testTruncate() throws Exception {
        int numMsgs = 20;
        for (int i = 0; i < numMsgs; i++) {
            cq.push("xxx_" + i);
        }

        for (int i = 0; i < numMsgs / 2; i++) {
            cq.pop();
        }

        cq.truncate();

        verifyWaitingQueue(0);
        verifyDeliveredQueue(0);
    }

    @Test
    public void testSimultaneousSinglePusherSinglePopper() {
        cq.setNearFifoOk(true);
        assertPushersPoppersWork(1, 1, 1000, 1000, 0, 0);

        // int numToPush = 100;
        // int numToPop = 100;
        // long pushDelay = 0;
        // long popDelay = 0;
        //
        // Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();
        //
        // CassQueuePusher cqPusher = new CassQueuePusher(cq, "test");
        // CassQueuePopper cqPopper = new CassQueuePopper(cq, "test", popQ);
        // cqPusher.start(numToPush, pushDelay);
        // cqPopper.start(numToPop, popDelay);
        //
        // Set<CassQMsg> msgSet = new LinkedHashSet<CassQMsg>();
        // Set<String> valueSet = new HashSet<String>();
        // while (!popQ.isEmpty() || !cqPusher.isFinished() ||
        // !cqPopper.isFinished()) {
        // CassQMsg qMsg = !popQ.isEmpty() ? popQ.remove() : null;
        // if (null != qMsg) {
        // if (msgSet.contains(qMsg)) {
        // fail("msg already popped - either message pushed twice or popped twice : "
        // + qMsg.toString());
        // }
        // else if (valueSet.contains(qMsg.getValue())) {
        // fail("value of message pushed more than once : " + qMsg.toString());
        // }
        //
        // msgSet.add(qMsg);
        // valueSet.add(qMsg.getValue());
        // }
        // else {
        // System.out.println("current elapsed pop time : " +
        // (cqPopper.getElapsedTime()) / 1000.0 + " ("
        // + cqPopper.getMsgsProcessed() + ")");
        // try {
        // Thread.sleep(200);
        // }
        // catch (InterruptedException e) {
        // // do nothing
        // }
        // }
        // }
        //
        // double popSecs = cqPopper.getElapsedTime() / 1000.0;
        // double pushSecs = cqPusher.getElapsedTime() / 1000.0;
        // System.out.println("total elapsed pusher time : " + popSecs);
        // System.out.println("total elapsed pop time : " + popSecs);
        // System.out.println("pushes/sec = " + numToPush / pushSecs);
        // System.out.println("pops/sec = " + numToPop / popSecs);
        //
        // assertTrue("expected pusher to be finished", cqPusher.isFinished());
        // assertTrue("expected popper to be finished", cqPopper.isFinished());
        // assertEquals("did not push the expected number of messages",
        // numToPush, cqPusher.getMsgsProcessed());
        // assertEquals("did not pop the expected number of messages", numToPop,
        // cqPopper.getMsgsProcessed());
        //
        // assertEquals("expected to have a total of " + numToPop +
        // " messages in set", numToPop, msgSet.size());
        // assertEquals("expected to have a total of " + numToPop +
        // " values in set", numToPop, valueSet.size());
    }

    @Test
    public void testSimultaneousMultiplePusherMultiplePopper() {
        cq.setNearFifoOk(true);
        assertPushersPoppersWork(4, 4, 1000, 1000, 0, 0);
    }

    @Test
    public void testShutdown() {
        cq.shutdown();
    }

    @Test
    public void testStartStopOfQueue() {
        fail("not implemented");
    }

    // -----------------------

    private void assertPushersPoppersWork(int numPushers, int numPoppers, int numToPushPerPusher,
            int numToPopPerPopper, long pushDelay, long popDelay) {

        Set<CassQMsg> msgSet = new LinkedHashSet<CassQMsg>();
        Set<String> valueSet = new HashSet<String>();
        Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();

        //
        // start a set of pushers and poppers
        //
        EnvProperties tmpProps = baseEnvProps.clone();
        tmpProps.setNumPushers(numPushers);
        tmpProps.setPushDelay(pushDelay);
        tmpProps.setNumMsgsPerPusher(numToPushPerPusher);
        tmpProps.setNumPoppers(numPoppers);
        tmpProps.setPopDelay(popDelay);
        tmpProps.setNumMsgsPerPopper(numToPopPerPopper);

        List<PushPopAbstractBase> pusherSet = testUtils.startPushers(cq, "test", tmpProps);
        List<PushPopAbstractBase> popperSet = testUtils.startPoppers(cq, "test", popQ, tmpProps);

        boolean finishedProperly = testUtils.monitorPushersPoppers(popQ, pusherSet, popperSet, msgSet, valueSet);

        assertTrue("monitoring of pushers/poppers finished improperly", finishedProperly);

        // double popSecs = cqPopper.getElapsedTime() / 1000.0;
        // double pushSecs = cqPusher.getElapsedTime() / 1000.0;
        // System.out.println("total elapsed pusher time : " + popSecs);
        // System.out.println("total elapsed pop time : " + popSecs);
        // System.out.println("pushes/sec = " + numToPush / pushSecs);
        // System.out.println("pops/sec = " + numToPop / popSecs);
        //
        assertTrue("expected pusher to be finished", testUtils.isPushPopOpFinished(pusherSet));
        assertTrue("expected popper to be finished", testUtils.isPushPopOpFinished(popperSet));

        int totalPushed = 0;
        for (PushPopAbstractBase pusher : pusherSet) {
            totalPushed += pusher.getMsgsProcessed();
        }
        int totalPopped = 0;
        for (PushPopAbstractBase popper : popperSet) {
            totalPopped += popper.getMsgsProcessed();
        }
        assertEquals("did not push the expected number of messages", (numPushers * numToPushPerPusher), totalPushed);
        assertEquals("did not pop the expected number of messages", (numPoppers * numToPopPerPopper), totalPopped);

        assertEquals("expected to have a total of " + (numPoppers * numToPopPerPopper) + " messages in set",
                (numPoppers * numToPopPerPopper), msgSet.size());
        assertEquals("expected to have a total of " + (numPoppers * numToPopPerPopper) + " values in set",
                (numPoppers * numToPopPerPopper), valueSet.size());
    }

    private void verifyExistsInDeliveredQueue(int index, int numMsgs, boolean wantExists) throws Exception {
        List<Column> colList = cq.getDeliveredMessages(index % cq.getNumPipes(), numMsgs + 1);
        if (wantExists) {
            boolean found = false;
            for (Column col : colList) {
                if (new String(col.getValue()).equals("xxx_" + index)) {
                    found = true;
                    break;
                }
            }
            assertTrue("should have found value, xxx_" + index + " in delivered queue", found);
        }
        else {
            for (Column col : colList) {
                assertNotSame(new String(col.getValue()), "xxx_" + index);
            }
        }

    }

    private void verifyExistsInWaitingQueue(int index, int numMsgs, boolean wantExists) throws Exception {
        List<Column> colList = cq.getWaitingMessages(index % cq.getNumPipes(), numMsgs + 1);
        if (wantExists) {
            boolean found = false;
            for (Column col : colList) {
                if (new String(col.getValue()).equals("xxx_" + index)) {
                    found = true;
                    break;
                }
            }
            assertTrue("should have found value, xxx_" + index + " in waiting queue", found);
        }
        else {
            for (Column col : colList) {
                assertNotSame(new String(col.getValue()), "xxx_" + index);
            }
        }

    }

    private void verifyDeliveredQueue(int numMsgs) throws Exception {
        QueueDescriptor qDesc = qRep.getQueueDescriptor(TestUtils.QUEUE_NAME);

        long startPipe = qDesc.getPopStartPipe();
        int min = numMsgs / cq.getNumPipes();
        int mod = numMsgs % cq.getNumPipes();

        for (int i = 0; i < cq.getNumPipes(); i++) {
            List<Column> colList = cq.getDeliveredMessages(startPipe + i, numMsgs + 1);
            assertEquals("count on queue index " + (startPipe + i) + " is incorrect", i < mod ? min + 1 : min,
                    colList.size());

            for (int j = 0; j < colList.size(); j++) {
                String value = new String(colList.get(j).getValue());
                assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
            }
        }
    }

    private void verifyWaitingQueue(int numMsgs) throws Exception {
        QueueDescriptor qDesc = qRep.getQueueDescriptor(TestUtils.QUEUE_NAME);

        long startPipe = qDesc.getPushStartPipe();
        long min = numMsgs / cq.getNumPipes();
        long mod = numMsgs % cq.getNumPipes();

        for (int i = 0; i < cq.getNumPipes(); i++) {
            List<Column> colList = cq.getWaitingMessages(startPipe + i, numMsgs + 1);
            assertEquals("count on queue index " + (startPipe + i) + " is incorrect", i < mod ? min + 1 : min,
                    colList.size());

            for (int j = 0; j < colList.size(); j++) {
                String value = new String(colList.get(j).getValue());
                assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
            }
        }
    }

    private static void setupEnvProperties() {

        Properties rawProps = new Properties();
        rawProps.setProperty("numPipes", "4");
        rawProps.setProperty("pushPipeIncrementDelay", "20000");
        rawProps.setProperty("hosts", "localhost");
        rawProps.setProperty("thriftPort", "9161");
        rawProps.setProperty("useFramedTransport", "false");
        rawProps.setProperty("minCacheConnsPerHost", "1");
        rawProps.setProperty("maxConnsPerHost", "20");
        rawProps.setProperty("targetConnsPerHost", "10");
        rawProps.setProperty("killNodeConnsOnException", "false");
        rawProps.setProperty("dropKeyspace", "true");
        rawProps.setProperty("replicationFactor", "1");
        rawProps.setProperty("dropKeyspace", "true");
        rawProps.setProperty("truncateQueue", "true");

        baseEnvProps = new EnvProperties(rawProps);
    }

    @Before
    public void setupQueue() throws Exception {
        cq = TestUtils.setupQueue(qRep, TestUtils.QUEUE_NAME, baseEnvProps, true, false);
        testUtils = new TestUtils(cq);
    }

    @BeforeClass
    public static void setupCassandraAndPelopsPool() throws Exception {
        setupEnvProperties();
        startCassandraInstance();

        qRep = TestUtils.setupQueueSystemAndPelopsPool(baseEnvProps, ConsistencyLevel.ONE);
    }

    @AfterClass
    public static void shutdownPelopsPool() throws Exception {
        Pelops.shutdown();
    }

    private static void startCassandraInstance() throws TTransportException, IOException, InterruptedException,
            SecurityException, IllegalArgumentException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException {
        CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
        cleaner.prepare();
        cassandra = new EmbeddedCassandraService();
        try {
            cassandra.init();
        }
        catch (TTransportException e) {
            logger.error("exception while initializing cassandra server", e);
            throw e;
        }
        Thread t = new Thread(cassandra);
        t.setDaemon(true);
        t.start();
        logger.info("setup: started embedded cassandra thread");
        Thread.sleep(100);
    }

}
