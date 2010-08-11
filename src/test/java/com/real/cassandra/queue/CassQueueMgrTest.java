package com.real.cassandra.queue;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.wyki.cassandra.pelops.GeneralPolicy;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.ThriftPoolComplex.Policy;

import com.real.cassandra.queue.raw.QueueRepository;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
    "classpath:spring-config-properties.xml" })
public class CassQueueMgrTest {
    private static final String POOL_NAME = "queueTest";
    private static final String QUEUE_NAME = "test";
    private static final int QUEUE_WIDTH = 4;

    private QueueRepository qMgr;
    private CassQueue cq;

    @Test
    public void testPush() throws Exception {
        int numEvents = 10;
        for (int i = 0; i < numEvents; i++) {
            cq.push("xxx_" + i);
        }

        verifyWaitingQueue(numEvents);
        verifyDeliveredQueue(0);
    }

    @Test
    public void testPop() throws Exception {
        int numEvents = 100;
        for (int i = 0; i < numEvents; i++) {
            cq.push("xxx_" + i);
        }

        ArrayList<Event> popList = new ArrayList<Event>(numEvents);
        Event evt;
        while (null != (evt = cq.pop())) {
            popList.add(evt);
        }

        assertEquals("did not pop the correct amount", numEvents, popList.size());
        for (int i = 0; i < numEvents; i++) {
            assertEquals("events were popped out of order", "xxx_" + i, popList.get(i).getValue());
        }

        verifyWaitingQueue(0);
        verifyDeliveredQueue(numEvents);
    }

    @Test
    public void testCommit() throws Exception {
        int numEvents = 10;
        for (int i = 0; i < numEvents; i++) {
            cq.push("xxx_" + i);
        }

        ArrayList<Event> popList = new ArrayList<Event>(numEvents);
        Event evt;
        while (null != (evt = cq.pop())) {
            popList.add(evt);
        }

        for (int i = 0; i < numEvents; i += 2) {
            cq.commit(popList.get(i));
        }

        verifyWaitingQueue(0);

        for (int i = 0; i < numEvents; i++) {
            verifyExistsInDeliveredQueue(i, numEvents, 0 != i % 2);
        }
    }

    @Test
    public void testRollback() throws Exception {
        int numEvents = 10;
        for (int i = 0; i < numEvents; i++) {
            cq.push("xxx_" + i);
        }

        ArrayList<Event> popList = new ArrayList<Event>(numEvents);
        Event evt;
        while (null != (evt = cq.pop())) {
            popList.add(evt);
        }

        for (int i = 0; i < numEvents; i += 2) {
            cq.rollback(popList.get(i));
        }

        for (int i = 0; i < numEvents; i++) {
            verifyExistsInWaitingQueue(i, numEvents, 0 == i % 2);
            verifyExistsInDeliveredQueue(i, numEvents, 0 != i % 2);
        }
    }

    @Test
    public void testRollbackAndPopAgain() throws Exception {
        cq.push("xxx");
        cq.push("yyy");
        cq.push("zzz");

        Event evtToRollback = cq.pop();

        Event evt = cq.pop();
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

    // -----------------------

    private void verifyExistsInDeliveredQueue(int index, int numEvents, boolean wantExists) throws Exception {
        List<Column> colList = qMgr.getDeliveredEvents(QUEUE_NAME, index % QUEUE_WIDTH, numEvents + 1);
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

    private void verifyExistsInWaitingQueue(int index, int numEvents, boolean wantExists) throws Exception {
        List<Column> colList = qMgr.getWaitingEvents(QUEUE_NAME, index % QUEUE_WIDTH, numEvents + 1);
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

    private void verifyDeliveredQueue(int numEvents) throws Exception {
        int min = numEvents / QUEUE_WIDTH;
        int mod = numEvents % QUEUE_WIDTH;

        for (int i = 0; i < QUEUE_WIDTH; i++) {
            List<Column> colList = qMgr.getDeliveredEvents(QUEUE_NAME, i, numEvents + 1);
            assertEquals("count on queue index " + i + " is incorrect", i < mod ? min + 1 : min, colList.size());

            for (int j = 0; j < colList.size(); j++) {
                String value = new String(colList.get(j).getValue());
                assertEquals("xxx_" + (i + (j * QUEUE_WIDTH)), value);
            }
        }
    }

    private void verifyWaitingQueue(int numEvents) throws Exception {
        int min = numEvents / QUEUE_WIDTH;
        int mod = numEvents % QUEUE_WIDTH;

        for (int i = 0; i < QUEUE_WIDTH; i++) {
            List<Column> colList = qMgr.getWaitingEvents(QUEUE_NAME, i, numEvents + 1);
            assertEquals("count on queue index " + i + " is incorrect", i < mod ? min + 1 : min, colList.size());

            for (int j = 0; j < colList.size(); j++) {
                String value = new String(colList.get(j).getValue());
                assertEquals("xxx_" + (i + (j * QUEUE_WIDTH)), value);
            }
        }
    }

    @Before
    public void setupQueueMgrAndPool() throws Exception {
        qMgr = new QueueRepository(POOL_NAME, ConsistencyLevel.ONE);
        qMgr.initCassandra(true);
        cq = qMgr.createQueue(QUEUE_NAME, QUEUE_WIDTH);
    }

    @BeforeClass
    public static void createPelopsPool() {
        GeneralPolicy generalPolicy = new GeneralPolicy();
        generalPolicy.setMaxOpRetries(10);
        Policy poolPolicy = new Policy();
        poolPolicy.setFramedTransportRequired(true);
        poolPolicy.setKillNodeConnsOnException(false);
        poolPolicy.setMaxConnectionsPerNode(2);
        poolPolicy.setMinCachedConnectionsPerNode(2);
        poolPolicy.setTargetConnectionsPerNode(2);

        Pelops.addPool(POOL_NAME, new String[] {
            "localhost" }, 9160, 5000, false, QueueRepository.KEYSPACE_NAME, generalPolicy, poolPolicy);
    }

    @AfterClass
    public static void shutdownQueueMgrAndPool() {
        Pelops.shutdown();
    }

}
