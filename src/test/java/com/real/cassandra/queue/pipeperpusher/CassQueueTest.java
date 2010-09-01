package com.real.cassandra.queue.pipeperpusher;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.cassandra.pelops.Pelops;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.EnvProperties;

/**
 * Unit tests for {@link CassQueueImpl}.
 * 
 * @author Todd Burruss
 */
public class CassQueueTest {
    private static Logger logger = LoggerFactory.getLogger(CassQueueTest.class);

    private static ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    private static QueueRepositoryImpl qRepos;
    private static CassQueueFactoryImpl cqFactory;
    private static EnvProperties baseEnvProps;

    private CassQueueImpl cq;

    // private TestUtils testUtils;

    @Test
    public void testPush() throws Exception {
        PusherImpl pusher = cqFactory.createPusher(cq);
        int numMsgs = 10;
        for (int i = 0; i < numMsgs; i++) {
            pusher.push("xxx_" + i);
        }

        // verifyWaitingQueue(numMsgs);
        // verifyDeliveredQueue(0);
    }

    // @Test
    // public void testPop() throws Exception {
    // cq.setStopPipeWatcher();
    //
    // int numMsgs = 100;
    // for (int i = 0; i < numMsgs; i++) {
    // cq.push("xxx_" + i);
    // }
    //
    // ArrayList<CassQMsg> popList = new ArrayList<CassQMsg>(numMsgs);
    // CassQMsg qMsg;
    // while (0 < qRep.getCount(cq.getName())) {
    // if (null != (qMsg = cq.pop())) {
    // popList.add(qMsg);
    // }
    // }
    //
    // assertEquals("did not pop the correct amount", numMsgs, popList.size());
    // for (int i = 0; i < numMsgs; i++) {
    // assertEquals("events were popped out of order", "xxx_" + i,
    // popList.get(i).getValue());
    // }
    //
    // verifyWaitingQueue(0);
    // verifyDeliveredQueue(numMsgs);
    // }
    //
    // @Test
    // public void testCommit() throws Exception {
    // int numMsgs = 10;
    // for (int i = 0; i < numMsgs; i++) {
    // cq.push("xxx_" + i);
    // }
    //
    // ArrayList<CassQMsg> popList = new ArrayList<CassQMsg>(numMsgs);
    // CassQMsg evt;
    // while (null != (evt = cq.pop())) {
    // popList.add(evt);
    // }
    //
    // for (int i = 0; i < numMsgs; i += 2) {
    // cq.commit(popList.get(i));
    // }
    //
    // verifyWaitingQueue(0);
    //
    // for (int i = 0; i < numMsgs; i++) {
    // verifyExistsInDeliveredQueue(i, numMsgs, 0 != i % 2);
    // }
    // }
    //
    // @Test
    // public void testRollback() throws Exception {
    // int numMsgs = 10;
    // for (int i = 0; i < numMsgs; i++) {
    // cq.push("xxx_" + i);
    // }
    //
    // ArrayList<CassQMsg> popList = new ArrayList<CassQMsg>(numMsgs);
    // CassQMsg evt;
    // while (null != (evt = cq.pop())) {
    // popList.add(evt);
    // }
    //
    // for (int i = 0; i < numMsgs; i += 2) {
    // cq.rollback(popList.get(i));
    // }
    //
    // for (int i = 0; i < numMsgs; i++) {
    // verifyExistsInWaitingQueue(i, numMsgs, 0 == i % 2);
    // verifyExistsInDeliveredQueue(i, numMsgs, 0 != i % 2);
    // }
    // }
    //
    // @Test
    // public void testRollbackAndPopAgain() throws Exception {
    // cq.setNearFifoOk(false);
    //
    // cq.push("xxx");
    // cq.push("yyy");
    // cq.push("zzz");
    //
    // CassQMsg evtToRollback = cq.pop();
    //
    // CassQMsg evt = cq.pop();
    // assertEquals("should have popped next event", "yyy", evt.getValue());
    // cq.commit(evt);
    //
    // cq.rollback(evtToRollback);
    //
    // evt = cq.pop();
    // assertEquals("should have popped rolled back event again", "xxx",
    // evt.getValue());
    // cq.commit(evt);
    //
    // evt = cq.pop();
    // assertEquals("should have popped last event", "zzz", evt.getValue());
    // cq.commit(evt);
    //
    // assertNull("should not be anymore events", cq.pop());
    //
    // verifyDeliveredQueue(0);
    // verifyWaitingQueue(0);
    // }
    //
    // @Test
    // public void testTruncate() throws Exception {
    // int numMsgs = 20;
    // for (int i = 0; i < numMsgs; i++) {
    // cq.push("xxx_" + i);
    // }
    //
    // for (int i = 0; i < numMsgs / 2; i++) {
    // cq.pop();
    // }
    //
    // cq.truncate();
    //
    // verifyWaitingQueue(0);
    // verifyDeliveredQueue(0);
    // }
    //
    // @Test
    // public void testSimultaneousSinglePusherSinglePopper() {
    // cq.setNearFifoOk(true);
    // assertPushersPoppersWork(1, 1, 1000, 1000, 0, 0);
    // }
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

    // private void assertPushersPoppersWork(int numPushers, int numPoppers, int
    // numToPushPerPusher,
    // int numToPopPerPopper, long pushDelay, long popDelay) {
    //
    // Set<CassQMsg> msgSet = new LinkedHashSet<CassQMsg>();
    // Set<String> valueSet = new HashSet<String>();
    // Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();
    //
    // //
    // // start a set of pushers and poppers
    // //
    // EnvProperties tmpProps = baseEnvProps.clone();
    // tmpProps.setNumPushers(numPushers);
    // tmpProps.setPushDelay(pushDelay);
    // tmpProps.setNumMsgsPerPusher(numToPushPerPusher);
    // tmpProps.setNumPoppers(numPoppers);
    // tmpProps.setPopDelay(popDelay);
    // tmpProps.setNumMsgsPerPopper(numToPopPerPopper);
    //
    // List<PushPopAbstractBase> pusherSet = testUtils.startPushers(cq, "test",
    // tmpProps);
    // List<PushPopAbstractBase> popperSet = testUtils.startPoppers(cq, "test",
    // popQ, tmpProps);
    //
    // boolean finishedProperly = testUtils.monitorPushersPoppers(popQ,
    // pusherSet, popperSet, msgSet, valueSet);
    //
    // assertTrue("monitoring of pushers/poppers finished improperly",
    // finishedProperly);
    // //
    // assertTrue("expected pusher to be finished",
    // testUtils.isPushPopOpFinished(pusherSet));
    // assertTrue("expected popper to be finished",
    // testUtils.isPushPopOpFinished(popperSet));
    //
    // int totalPushed = 0;
    // for (PushPopAbstractBase pusher : pusherSet) {
    // totalPushed += pusher.getMsgsProcessed();
    // }
    // int totalPopped = 0;
    // for (PushPopAbstractBase popper : popperSet) {
    // totalPopped += popper.getMsgsProcessed();
    // }
    // assertEquals("did not push the expected number of messages", (numPushers
    // * numToPushPerPusher), totalPushed);
    // assertEquals("did not pop the expected number of messages", (numPoppers *
    // numToPopPerPopper), totalPopped);
    //
    // assertEquals("expected to have a total of " + (numPoppers *
    // numToPopPerPopper) + " messages in set",
    // (numPoppers * numToPopPerPopper), msgSet.size());
    // assertEquals("expected to have a total of " + (numPoppers *
    // numToPopPerPopper) + " values in set",
    // (numPoppers * numToPopPerPopper), valueSet.size());
    // }
    //
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
    public void setupQueue() throws Exception {
        String qName = "test-" + System.currentTimeMillis();
        cq = (CassQueueImpl) cqFactory.createQueueInstance(qName, 10000, 2000, 1, false);
    }

    @BeforeClass
    public static void setupCassandraAndPelopsPool() throws Exception {
        baseEnvProps = TestUtils.createEnvPropertiesWithDefaults();

        TestUtils.startCassandraInstance();

        qRepos = TestUtils.createQueueRepository(baseEnvProps, consistencyLevel);
    }

    @AfterClass
    public static void shutdownPelopsPool() throws Exception {
        Pelops.shutdown();
    }

}
