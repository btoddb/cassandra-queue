package com.real.cassandra.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Column;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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
public class CassQueueTest {
    private static Logger logger = LoggerFactory.getLogger(CassQueueTest.class);

    private static PelopsPool queuePool;
    private static PelopsPool systemPool;
    private static QueueRepository qRep;
    private static EmbeddedCassandraService cassandra;

    private CassQueue cq;
    private TestUtils testUtils;

    @Test
    public void testPush() throws Exception {
        int numMsgs = 10;
        for (int i = 0; i < numMsgs; i++) {
            cq.push("xxx_" + i);
        }

        verifyWaitingQueue(numMsgs);
        verifyDeliveredQueue(0);
    }

    @Test
    public void testPop() throws Exception {
        int numMsgs = 100;
        for (int i = 0; i < numMsgs; i++) {
            cq.push("xxx_" + i);
        }

        ArrayList<CassQMsg> popList = new ArrayList<CassQMsg>(numMsgs);
        CassQMsg evt;
        while (null != (evt = cq.pop())) {
            popList.add(evt);
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

        int numToPush = 100;
        int numToPop = 100;
        long pushDelay = 0;
        long popDelay = 0;

        Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();

        CassQueuePusher cqPusher = new CassQueuePusher(cq, "test");
        CassQueuePopper cqPopper = new CassQueuePopper(cq, "test", popQ);
        cqPusher.start(numToPush, pushDelay);
        cqPopper.start(numToPop, popDelay);

        Set<CassQMsg> msgSet = new LinkedHashSet<CassQMsg>();
        Set<String> valueSet = new HashSet<String>();
        while (!popQ.isEmpty() || !cqPusher.isFinished() || !cqPopper.isFinished()) {
            CassQMsg qMsg = !popQ.isEmpty() ? popQ.remove() : null;
            if (null != qMsg) {
                if (msgSet.contains(qMsg)) {
                    fail("msg already popped - either message pushed twice or popped twice : " + qMsg.toString());
                }
                else if (valueSet.contains(qMsg.getValue())) {
                    fail("value of message pushed more than once : " + qMsg.toString());
                }

                msgSet.add(qMsg);
                valueSet.add(qMsg.getValue());
            }
            else {
                System.out.println("current elapsed pop time : " + (cqPopper.getElapsedTime()) / 1000.0 + " ("
                        + cqPopper.getMsgsProcessed() + ")");
                try {
                    Thread.sleep(200);
                }
                catch (InterruptedException e) {
                    // do nothing
                }
            }
        }

        double popSecs = cqPopper.getElapsedTime() / 1000.0;
        double pushSecs = cqPusher.getElapsedTime() / 1000.0;
        System.out.println("total elapsed pusher time : " + popSecs);
        System.out.println("total elapsed pop time : " + popSecs);
        System.out.println("pushes/sec = " + numToPush / pushSecs);
        System.out.println("pops/sec = " + numToPop / popSecs);

        assertTrue("expected pusher to be finished", cqPusher.isFinished());
        assertTrue("expected popper to be finished", cqPopper.isFinished());
        assertEquals("did not push the expected number of messages", numToPush, cqPusher.getMsgsProcessed());
        assertEquals("did not pop the expected number of messages", numToPop, cqPopper.getMsgsProcessed());

        assertEquals("expected to have a total of " + numToPop + " messages in set", numToPop, msgSet.size());
        assertEquals("expected to have a total of " + numToPop + " values in set", numToPop, valueSet.size());
    }

    @Test
    public void testSimultaneousMultiplePusherMultiplePopper() {
        cq.setNearFifoOk(true);

        int numPushers = 4;
        int numPoppers = 4;
        int numToPushPerPusher = 100;
        int numToPopPerPopper = 100;
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
        int numPopped = 0;
        while (!popQ.isEmpty() || !testUtils.isPushPopOpFinished(popperSet)
                || !testUtils.isPushPopOpFinished(pusherSet)) {
            CassQMsg qMsg = !popQ.isEmpty() ? popQ.remove() : null;
            if (null != qMsg) {
                numPopped++;
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

        assertEquals("did not pop the same number pushed", numPushers * numToPushPerPusher, numPopped);

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

    // -----------------------

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
        int min = numMsgs / cq.getNumPipes();
        int mod = numMsgs % cq.getNumPipes();

        for (int i = 0; i < cq.getNumPipes(); i++) {
            List<Column> colList = cq.getDeliveredMessages(i, numMsgs + 1);
            assertEquals("count on queue index " + i + " is incorrect", i < mod ? min + 1 : min, colList.size());

            for (int j = 0; j < colList.size(); j++) {
                String value = new String(colList.get(j).getValue());
                assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
            }
        }
    }

    private void verifyWaitingQueue(int numMsgs) throws Exception {
        int min = numMsgs / cq.getNumPipes();
        int mod = numMsgs % cq.getNumPipes();

        for (int i = 0; i < cq.getNumPipes(); i++) {
            List<Column> colList = cq.getWaitingMessages(i, numMsgs + 1);
            assertEquals("count on queue index " + i + " is incorrect", i < mod ? min + 1 : min, colList.size());

            for (int j = 0; j < colList.size(); j++) {
                String value = new String(colList.get(j).getValue());
                assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
            }
        }
    }

    @Before
    public void setupQueueMgrAndPool() {
        cq = new CassQueue(qRep, TestUtils.QUEUE_NAME, 4, true, false);
        testUtils = new TestUtils(cq);
        try {
            cq.truncate();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void setupCassandraAndPelopsPool() throws Exception {
        startCassandraInstance();

        // must create system pool first and initialize cassandra
        systemPool = TestUtils.createSystemPool(TestUtils.NODE_LIST, TestUtils.THRIFT_PORT);
        qRep = new QueueRepository(systemPool, TestUtils.REPLICATION_FACTOR, TestUtils.CONSISTENCY_LEVEL);
        qRep.initCassandra(true);

        queuePool = TestUtils.createQueuePool(1, 1);
        qRep.setQueuePool(queuePool);
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
