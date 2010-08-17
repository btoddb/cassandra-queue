package com.real.cassandra.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wyki.cassandra.pelops.Pelops;

import com.real.cassandra.queue.repository.PelopsPool;
import com.real.cassandra.queue.repository.QueueRepository;

/**
 * Unit tests for {@link CassQueue}.
 * 
 * @author Todd Burruss
 */
public class CassQueueTest {
    private static PelopsPool pool;
    private static QueueRepository qRep;

    private CassQueue cq;

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
    public void testSimultaneousPushPop() {
        cq.setNearFifoOk(true);
        int numToPush = 1000;
        int numToPop = 1000;
        long pushDelay = 0;
        long popDelay = 0;

        CassQueuePusher cqPusher = new CassQueuePusher(cq, "test");
        CassQueuePopper cqPopper = new CassQueuePopper(cq, "test");
        cqPusher.start(numToPush, pushDelay);
        cqPopper.start(numToPop, popDelay);

        Set<CassQMsg> msgSet = new LinkedHashSet<CassQMsg>();
        Set<String> valueSet = new HashSet<String>();
        List<CassQMsg> popList = cqPopper.getPopList();
        while (!popList.isEmpty() || !cqPusher.isFinished() || !cqPopper.isFinished()) {
            CassQMsg qMsg = !popList.isEmpty() ? popList.remove(0) : null;
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
                        + cqPopper.getMsgsPopped() + ")");
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
        assertEquals("did not push the expected number of messages", numToPush, cqPusher.getMsgsPushed());
        assertEquals("did not pop the expected number of messages", numToPop, cqPopper.getMsgsPopped());

        assertEquals("expected to have a total of " + numToPop + " messages in set", numToPop, msgSet.size());
        assertEquals("expected to have a total of " + numToPop + " values in set", numToPop, valueSet.size());
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
        try {
            cq.truncate();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void setupPelopsPool() throws Exception {
        pool = TestUtils.createPelopsPool(5, 5);
        qRep = new QueueRepository(pool, TestUtils.REPLICATION_FACTOR, TestUtils.CONSISTENCY_LEVEL);
        qRep.initCassandra(true);
    }

    @AfterClass
    public static void shutdownQueueMgrAndPool() {
        Pelops.shutdown();
    }

}
