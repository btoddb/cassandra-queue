package com.real.cassandra.queue.pipeperpusher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Before;
import org.junit.Test;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.pipeperpusher.utils.CassQueueUtils;
import com.real.cassandra.queue.pipeperpusher.utils.EnvProperties;

/**
 * Unit tests for {@link CassQueueImpl}.
 * 
 * @author Todd Burruss
 */
public class CassQueueTest extends PipePerPusherTestBase {
    // private static Logger logger =
    // LoggerFactory.getLogger(CassQueueTest.class);

    private static CassQueueFactoryImpl cqFactory;

    @Test
    public void testTruncate() throws Exception {
        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 2, 1, 5000, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper(false);

        int numMsgs = 20;
        for (int i = 0; i < numMsgs; i++) {
            pusher.push("xxx_" + i);
        }

        for (int i = 0; i < numMsgs / 2; i++) {
            popper.pop();
        }

        cq.truncate();
        // popper = cq.createPopper(false);

        assertEquals("all data should have been truncated", 0, qRepos.getCountOfWaitingMsgs(cq.getName()).totalMsgCount);
        assertEquals("all data should have been truncated", 0,
                qRepos.getCountOfDeliveredMsgs(cq.getName()).totalMsgCount);

        for (int i = 0; i < numMsgs; i++) {
            CassQMsg qMsg = popper.pop();
            assertNull("should not have found a msg - index = " + i + " : " + (null != qMsg ? qMsg.toString() : null),
                    qMsg);
        }
    }

    @Test
    public void testSimultaneousSinglePusherSinglePopper() throws Exception {
        assertPushersPoppersWork(1, 1, 1000, 0, 0);
    }

    @Test
    public void testSimultaneousSinglePusherMultiplePoppers() throws Exception {
        assertPushersPoppersWork(1, 4, 10000, 0, 0);
    }

    @Test
    public void testSimultaneousMultiplePushersMultiplePoppers() throws Exception {
        assertPushersPoppersWork(2, 4, 10000, 3, 0);
    }

    @Test
    public void testFinishedPipeRemoval() {
        fail("not implemented yet");
    }

    @Test
    public void testPusherDiesCleanupPipes() {
        fail("not implemented yet");
    }

    //
    // @Test
    // public void testSimultaneousMultiplePusherMultiplePopper() {
    // cq.setNearFifoOk(true);
    // assertPushersPoppersWork(4, 10, 10000, 4000, 2, 0);
    // }
    //
    // @Test
    // public void testShutdown() {
    // cq.shutdown();
    // }
    //
    // @Test
    // public void testStartStopOfQueue() {
    // fail("not implemented");
    // }

    // -----------------------

    private void assertPushersPoppersWork(int numPushers, int numPoppers, int numMsgs, long pushDelay, long popDelay)
            throws Exception {
        Set<CassQMsg> msgSet = new LinkedHashSet<CassQMsg>();
        Set<String> valueSet = new HashSet<String>();
        Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();

        //
        // start a set of pushers and poppers
        //

        EnvProperties tmpProps = baseEnvProps.clone();
        tmpProps.setNumMsgs(numMsgs);
        tmpProps.setNumPushers(numPushers);
        tmpProps.setPushDelay(pushDelay);
        // tmpProps.setNumMsgsPerPusher(numToPushPerPusher);
        tmpProps.setNumPoppers(numPoppers);
        tmpProps.setPopDelay(popDelay);
        // tmpProps.setNumMsgsPerPopper(numToPopPerPopper);

        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 100, 4, 5000, false);
        List<PushPopAbstractBase> pusherSet = CassQueueUtils.startPushers(cq, tmpProps);
        List<PushPopAbstractBase> popperSet = CassQueueUtils.startPoppers(cq, popQ, tmpProps);

        boolean finishedProperly = CassQueueUtils.monitorPushersPoppers(popQ, pusherSet, popperSet, msgSet, valueSet);

        assertTrue("monitoring of pushers/poppers finished improperly", finishedProperly);

        assertTrue("expected pusher to be finished", CassQueueUtils.isPushPopOpFinished(pusherSet));
        assertTrue("expected popper to be finished", CassQueueUtils.isPushPopOpFinished(popperSet));

        int totalPushed = 0;
        for (PushPopAbstractBase pusher : pusherSet) {
            totalPushed += pusher.getMsgsProcessed();
        }
        int totalPopped = 0;
        for (PushPopAbstractBase popper : popperSet) {
            totalPopped += popper.getMsgsProcessed();
        }
        assertEquals("did not push the expected number of messages", numMsgs, totalPushed);
        assertEquals("did not pop the expected number of messages", numMsgs, totalPopped);

        assertEquals("expected to have a total of " + numMsgs + " messages in set", numMsgs, msgSet.size());
        assertEquals("expected to have a total of " + numMsgs + " values in set", numMsgs, valueSet.size());

        assertEquals("waiting queue should be empty", 0, qRepos.getCountOfWaitingMsgs(cq.getName()).totalMsgCount);
        assertEquals("delivered queue should be empty", 0, qRepos.getCountOfDeliveredMsgs(cq.getName()).totalMsgCount);
    }

    // private void verifyExistsInDeliveredQueue(int index, int numMsgs, boolean
    // wantExists) throws Exception {
    // List<Column> colList = cq.getDeliveredMessages(index % cq.getNumPipes(),
    // numMsgs + 1);
    // if (wantExists) {
    // boolean found = false;
    // for (Column col : colList) {
    // if (new String(col.getValue()).equals("xxx_" + index)) {
    // found = true;
    // break;
    // }
    // }
    // assertTrue("should have found value, xxx_" + index +
    // " in delivered queue", found);
    // }
    // else {
    // for (Column col : colList) {
    // assertNotSame(new String(col.getValue()), "xxx_" + index);
    // }
    // }
    //
    // }
    //
    // private void verifyExistsInWaitingQueue(int pipeNum, int numMsgs, boolean
    // wantExists) throws Exception {
    // List<Column> colList =
    // cq.getWaitingMessages(pipeMgr.getPipeDescriptor(pipeNum %
    // cq.getNumPipes()), numMsgs + 1);
    // if (wantExists) {
    // boolean found = false;
    // for (Column col : colList) {
    // if (new String(col.getValue()).equals("xxx_" + pipeNum)) {
    // found = true;
    // break;
    // }
    // }
    // assertTrue("should have found value, xxx_" + pipeNum +
    // " in waiting queue", found);
    // }
    // else {
    // for (Column col : colList) {
    // assertNotSame(new String(col.getValue()), "xxx_" + pipeNum);
    // }
    // }
    //
    // }
    //
    // private void verifyDeliveredQueue(int numMsgs) throws Exception {
    // QueueDescriptor qDesc = qRep.getQueueDescriptor(TestUtils.QUEUE_NAME);
    //
    // long startPipe = qDesc.getPopStartPipe();
    // int min = numMsgs / cq.getNumPipes();
    // int mod = numMsgs % cq.getNumPipes();
    //
    // for (int i = 0; i < cq.getNumPipes(); i++) {
    // List<Column> colList = cq.getDeliveredMessages(startPipe + i, numMsgs +
    // 1);
    // assertEquals("count on queue index " + (startPipe + i) + " is incorrect",
    // i < mod ? min + 1 : min,
    // colList.size());
    //
    // for (int j = 0; j < colList.size(); j++) {
    // String value = new String(colList.get(j).getValue());
    // assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
    // }
    // }
    // }
    //
    // private void verifyWaitingQueue(int numMsgs) throws Exception {
    // QueueDescriptor qDesc = qRep.getQueueDescriptor(TestUtils.QUEUE_NAME);
    //
    // long startPipe = qDesc.getPushStartPipe();
    // long min = numMsgs / cq.getNumPipes();
    // long mod = numMsgs % cq.getNumPipes();
    //
    // for (int i = 0; i < cq.getNumPipes(); i++) {
    // List<Column> colList =
    // cq.getWaitingMessages(pipeMgr.getPipeDescriptor(startPipe + i), numMsgs +
    // 1);
    // assertEquals("count on queue index " + (startPipe + i) + " is incorrect",
    // i < mod ? min + 1 : min,
    // colList.size());
    //
    // for (int j = 0; j < colList.size(); j++) {
    // String value = new String(colList.get(j).getValue());
    // assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
    // }
    // }
    // }

    @Before
    public void setupTest() throws Exception {
        cqFactory = new CassQueueFactoryImpl(qRepos, new PipeDescriptorFactory(qRepos), new PipeLockerImpl());
    }

}
