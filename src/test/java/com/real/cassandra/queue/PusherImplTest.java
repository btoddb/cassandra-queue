package com.real.cassandra.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.real.cassandra.queue.app.CassQueueApp;
import com.real.cassandra.queue.locks.LocalLockerImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;

public class PusherImplTest extends CassQueueTestBase {
    private CassQueueImpl cq;
    private CassQueueFactoryImpl cqFactory;

    @Test
    public void testSinglePusherSingleMsg() throws Exception {
        cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 10, 1, 5000, false);
        PusherImpl pusher = cq.createPusher();
        String msgData = "the data-" + System.currentTimeMillis();

        CassQMsg qMsg = pusher.push(msgData);

        assertEquals("stats should show only one msg", 1, qMsg.getPipeDescriptor().getPushCount());
        assertNotNull(qMsg.getMsgId());
        assertNotNull(qMsg.getPipeDescriptor().getPipeId());
        assertNotNull(cq.getName(), qMsg.getPipeDescriptor().getQName());
        assertEquals(msgData, qMsg.getMsgData());

        CassQMsg qMsgNew = qRepos.getMsg(cq.getName(), qMsg.getPipeDescriptor(), qMsg.getMsgId());

        assertNotNull("should have created new message in the expected queue and pipe", qMsgNew);
        assertEquals("didn't seem to write the correct data to the correct place", qMsg.getMsgData(),
                qMsgNew.getMsgData());
    }

    @Test
    public void testMultiplePushers() throws Exception {
        int maxPushesPerPipe = 10;
        cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe, 1, 5000, false);
        int numMsgs = 10;
        ArrayList<CassQMsg> pushList1 = new ArrayList<CassQMsg>(numMsgs);
        ArrayList<CassQMsg> pushList2 = new ArrayList<CassQMsg>(numMsgs);

        PusherImpl pusher1 = cq.createPusher();
        PusherImpl pusher2 = cq.createPusher();

        for (int i = 0; i < numMsgs; i++) {
            pushList1.add(pusher1.push("push1-" + System.currentTimeMillis() + i));
            pushList2.add(pusher2.push("push2-" + System.currentTimeMillis() + i));
            Thread.sleep(1);
        }

        System.out.println("push1 = " + qRepos.getPipeDescriptor(pusher1.getPipeDesc().getPipeId()).getPushCount());
        System.out.println("push2 = " + qRepos.getPipeDescriptor(pusher2.getPipeDesc().getPipeId()).getPushCount());

        List<PipeDescriptorImpl> pipeList = qRepos.getOldestPopActivePipes(cq.getName(), 10);
        System.out.println("num pipes = " + pipeList.size());
        for (PipeDescriptorImpl pd : pipeList) {
            CassQueueApp.outputPipeDescription(qRepos, pd, maxPushesPerPipe);
        }

        assertNotSame(pusher1.getPipeDesc(), pusher2.getPipeDesc());
        PipeDescriptorImpl pipeDesc = qRepos.getPipeDescriptor(pusher1.getPipeDesc().getPipeId());
        assertEquals("should have inserted " + numMsgs + " into pipe : " + pipeDesc.toString(), numMsgs,
                pipeDesc.getPushCount());
        pipeDesc = qRepos.getPipeDescriptor(pusher2.getPipeDesc().getPipeId());
        assertEquals("should have inserted " + numMsgs + " into pipe", numMsgs, pipeDesc.getPushCount());

        for (int i = 0; i < numMsgs; i++) {
            CassQMsg qMsg = pushList1.get(i);
            CassQMsg qMsgNew = qRepos.getMsg(cq.getName(), qMsg.getPipeDescriptor(), qMsg.getMsgId());
            assertEquals(qMsg.getMsgData(), qMsgNew.getMsgData());
        }

        for (int i = 0; i < numMsgs; i++) {
            CassQMsg qMsg = pushList2.get(i);
            CassQMsg qMsgNew = qRepos.getMsg(cq.getName(), qMsg.getPipeDescriptor(), qMsg.getMsgId());
            assertEquals(qMsg.getMsgData(), qMsgNew.getMsgData());
        }
    }

    @Test
    public void testRollAfterMaxPushes() throws Exception {
        int maxPushesPerPipe = 10;
        cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe, 1, 5000, false);
        int numMsgs = 30;
        ArrayList<CassQMsg> pushList = new ArrayList<CassQMsg>(numMsgs);

        PusherImpl pusher = cq.createPusher();

        for (int i = 0; i < numMsgs; i++) {
            pushList.add(pusher.push("push-" + System.currentTimeMillis() + ":" + i));
            Thread.sleep(1);
        }

        List<PipeDescriptorImpl> pipeList = qRepos.getOldestPopActivePipes(cq.getName(), 10);
        System.out.println("num pipes = " + pipeList.size());
        for (PipeDescriptorImpl pd : pipeList) {
            CassQueueApp.outputPipeDescription(qRepos, pd, maxPushesPerPipe);
        }

        Set<UUID> pipeSet = new HashSet<UUID>();
        UUID lastPipeId = null;
        for (int i = 0; i < numMsgs; i++) {
            CassQMsg qMsg = pushList.get(i);
            CassQMsg qMsgNew = qRepos.getMsg(cq.getName(), qMsg.getPipeDescriptor(), qMsg.getMsgId());
            assertEquals(qMsg.getMsgData(), qMsgNew.getMsgData());
            lastPipeId = qMsg.getPipeDescriptor().getPipeId();
            pipeSet.add(lastPipeId);
        }

        assertEquals("should have created exactly " + numMsgs / cq.getMaxPushesPerPipe() + " pipes",
                numMsgs / cq.getMaxPushesPerPipe(), pipeSet.size());

        for (UUID pipeId : pipeSet) {
            PipeDescriptorImpl pipeDesc = qRepos.getPipeDescriptor(pipeId);
            assertEquals("pipe " + pipeDesc.getPipeId() + " should have " + (numMsgs / pipeSet.size()) + " messages",
                    numMsgs / pipeSet.size(), pipeDesc.getPushCount());
            if (!pipeDesc.getPipeId().equals(lastPipeId)) {
                assertFalse("pipe should not be active", pipeDesc.isPushActive());
            }
            else {
                assertTrue("pipe should still be active", pipeDesc.isPushActive());
            }
        }
    }

    @Test
    public void testShutdownInProgress() throws Exception {
        cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 10, 1, 5000, false);
        PusherImpl pusher = cq.createPusher();
        int numMsgs = 45;
        Set<UUID> pipeSet = new HashSet<UUID>();
        for (int i = 0; i < numMsgs; i++) {
            CassQMsg qMsg = pusher.push("push-" + System.currentTimeMillis() + i);
            pipeSet.add(qMsg.getPipeDescriptor().getPipeId());
        }

        pusher.shutdown();

        try {
            pusher.push("push-" + System.currentTimeMillis() + numMsgs);
            fail("pusher should not have allowed a new message since in shutdown mode");
        }
        catch (IllegalStateException e) {
            // all good, expected
        }

        for (UUID pipeId : pipeSet) {
            PipeDescriptorImpl pipeDesc = qRepos.getPipeDescriptor(pipeId);
            assertFalse("all pipes should be inactive because in shutdown mode", pipeDesc.isPushActive());
        }
    }

    // -------------------------

    @Before
    public void setupTest() throws Exception {
        cqFactory = new CassQueueFactoryImpl(qRepos, new LocalLockerImpl(), new LocalLockerImpl());
    }
}
