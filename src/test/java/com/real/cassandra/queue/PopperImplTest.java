package com.real.cassandra.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;
import org.junit.Before;
import org.junit.Test;

import com.real.cassandra.queue.locks.LocalLockerImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.utils.MyIp;

public class PopperImplTest extends CassQueueTestBase {
    private CassQueueFactoryImpl cqFactory;

    @Test
    public void testSinglePopper() throws Exception {
        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 60000, 10, 30000, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();

        String msgData = "the data-" + System.currentTimeMillis();
        CassQMsg qMsgPush = pusher.push(msgData);
        assertNotNull( qMsgPush.getMsgDesc().getCreateTimestamp());
        
        CassQMsg qMsgPop = popper.pop();
        assertEquals("msg pushed isn't msg popped", qMsgPush, qMsgPop);
        assertEquals("msg pushed has different payload than msg popped", qMsgPush.getMsgDesc().getPayloadAsByteBuffer(),
                qMsgPop.getMsgDesc().getPayloadAsByteBuffer());
        assertNotNull( qMsgPop.getMsgDesc().getPopTimestamp() );
        assertEquals( qMsgPush.getMsgDesc().getCreateTimestamp(), qMsgPop.getMsgDesc().getCreateTimestamp() );
        
        List<PipeDescriptorImpl> pipeDescList = qRepos.getOldestPopActivePipes(cq.getName(), 1);
        PipeDescriptorImpl pipeDesc = pipeDescList.get(0);

        assertNull("msg should have been moved from waiting to pending", qRepos.getOldestMsgFromWaitingPipe(pipeDesc));
        CassQMsg qDelivMsg = qRepos.getOldestMsgFromPendingPipe(pipeDesc);
        assertEquals("msg pushed isn't msg in pending pipe", qMsgPush, qDelivMsg);
        assertEquals("msg pushed has different value than msg in pending pipe", qMsgPush.getMsgDesc()
                .getPayloadAsByteBuffer(), qDelivMsg.getMsgDesc().getPayloadAsByteBuffer());
        assertEquals(1, pipeDesc.getPushCount());
        assertEquals(1, pipeDesc.getPopCount());
    }

    @Test
    public void testTwoPushersTwoPoppers() throws Exception {
        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 10, 30000, false);
        PusherImpl pusher1 = cq.createPusher();
        PusherImpl pusher2 = cq.createPusher();
        PopperImpl popper1 = cq.createPopper();
        PopperImpl popper2 = cq.createPopper();
        int msgCount = 40;
        ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(msgCount);

        for (int i = 0; i < msgCount; i++) {
            if (0 == i % 2) {
                msgList.add(pusher1.push("data-" + System.currentTimeMillis() + "-" + i));
            }
            else {
                msgList.add(pusher2.push("data-" + System.currentTimeMillis() + "-" + i));
            }
        }

        for (int i = 0; i < msgCount; i++) {
            CassQMsg qPushMsg = msgList.get(i);
            CassQMsg qPopMsg;
            if ( 0 == i%2 ) {
                qPopMsg = popper1.pop();
            }
            else {
                qPopMsg = popper2.pop();
            }
            
            assertEquals("pushed msg not same as popped", qPushMsg, qPopMsg);
            assertEquals("pushed msg data not same as popped data", qPushMsg.getMsgDesc().getPayloadAsByteBuffer(),
                    qPopMsg.getMsgDesc().getPayloadAsByteBuffer());
        }

//        List<PipeDescriptorImpl> pipeDescList = qRepos.getOldestPopActivePipes(cq.getName(), 1);
//        pipeDesc = pipeDescList.get(0);
//
//        assertNull("all msgs should have been moved from waiting to pending", qRepos
//                .getOldestMsgFromWaitingPipe(pipeDesc));
//
//        for (int i = 0; i < msgCount; i++) {
//            CassQMsg qPushMsg = msgList.get(i);
//            CassQMsg qDelivMsg = qRepos.getOldestMsgFromPendingPipe(pipeDesc);
//            qRepos.removeMsgFromPendingPipe(qDelivMsg);
//            assertEquals("pushed msg not same as popped", qPushMsg, qDelivMsg);
//            assertEquals("pushed msg data not same as popped data", qPushMsg.getMsgDesc().getPayloadAsByteBuffer(),
//                    qDelivMsg.getMsgDesc().getPayloadAsByteBuffer());
//        }

    }

    @Test
    public void testSinglePusherSinglePopperMultiplePipes() throws Exception {
        int maxPushesPerPipe = 5;
        int numMsgs = 50;

        CassQueueImpl cq =
                cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe, 30000, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();

        for (int i = 0; i < numMsgs; i++) {
            pusher.push("data-" + i);
        }

        for (int i = 0; i < numMsgs; i++) {
            CassQMsg qMsg = popper.pop();
            assertNotNull("popped empty/null msg = " + i, qMsg);
            assertEquals("pushed msg data not same as popped data = " + i, "data-" + i, new String(qMsg.getMsgDesc()
                    .getPayload()));
        }
    }

    @Test
    public void testMarkingPipeAsPopCompleted() throws Exception {
        int maxPushesPerPipe = 10;
        CassQueueImpl cq =
                cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe, 30000, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();
        int msgCount = maxPushesPerPipe;

        for (int i = 0; i < msgCount; i++) {
            pusher.push("data-" + System.currentTimeMillis() + "-" + i);
        }
        pusher.push("over-to-next");
        PipeDescriptorImpl pipeDesc = null;

        for (int i = 0; i < msgCount; i++) {
            CassQMsg qMsg = popper.pop();
            pipeDesc = qRepos.getPipeDescriptor(qMsg.getPipeDescriptor().getPipeId());
            assertEquals("pop status should still be " + PipeStatus.ACTIVE, PipeStatus.ACTIVE, pipeDesc.getPopStatus());
        }

        // do one more pop to push over the edge to next pipe
        // popper.forceRefresh();
        assertEquals("should have rolled to next pipe and retrieved next msg", "over-to-next", new String(popper.pop()
                .getMsgDesc().getPayload()));

        cq.forcePipeReaperWakeUp();
        Thread.sleep(100);

        pipeDesc = qRepos.getPipeDescriptor(pipeDesc.getPipeId());
        assertNull("pipe descriptor should have been removed from system", pipeDesc);
    }

    @Test
    public void testNoPipes() throws Exception {
        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 10, 30000, false);
        PopperImpl popper = cq.createPopper();
        assertNull("should return null", popper.pop());
    }

    @Test
    public void testShutdownInProgress() throws Exception {
        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 10, 30000, false);
        PopperImpl popper = cq.createPopper();
        PusherImpl pusher = cq.createPusher();

        pusher.push("blah1");
        pusher.push("blah1");
        pusher.push("blah1");

        // popper.forceRefresh();
        popper.pop();
        popper.shutdownAndWait();

        try {
            popper.pop();
            fail("pusher should not have allowed a new message since in shutdown mode");
        }
        catch (IllegalStateException e) {
            // all good, expected
        }
    }

    @Test
    public void testCommit() throws Exception {
        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 10, 30000, false);
        PopperImpl popper = cq.createPopper();
        PusherImpl pusher = cq.createPusher();
        int numMsgs = 3;
        ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(numMsgs);

        for (int i = 0; i < numMsgs; i++) {
            pusher.push("blah");
            msgList.add(popper.pop());
        }

        // popper.forceRefresh();
        for (int i = numMsgs; 0 < i; i--) {
            List<PipeDescriptorImpl> pdList = qRepos.getOldestPopActivePipes(cq.getName(), numMsgs * 2);
            PipeDescriptorImpl pipeDesc = pdList.get(0);
            assertEquals("as messages are commited, should be removed from pending pipe", i, qRepos
                    .getPendingMessagesFromPipe(pipeDesc, numMsgs * 2).size());
            popper.commit(msgList.remove(0));
        }

        List<PipeDescriptorImpl> pdList = qRepos.getOldestPopActivePipes(cq.getName(), numMsgs * 2);
        PipeDescriptorImpl pipeDesc = pdList.get(0);
        assertEquals("commiting is complete, shouldn't be any messages in pending pipe", 0, qRepos
                .getPendingMessagesFromPipe(pipeDesc, numMsgs * 2).size());
        assertEquals("commiting is complete, shouldn't be any messages in waiting pipe", 0, qRepos
                .getWaitingMessagesFromPipe(pipeDesc, numMsgs * 2).size());
    }

    @Test
    public void testRollback() throws Exception {
        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 10, 30000, false);
        PopperImpl popper = cq.createPopper();
        PusherImpl pusher = cq.createPusher();
        int numMsgs = 6;
        ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(numMsgs);

        for (int i = 0; i < numMsgs; i++) {
            pusher.push("blah");
            msgList.add(popper.pop());
        }

        ArrayList<CassQMsg> rollbackList = new ArrayList<CassQMsg>(numMsgs / 2);
        for (int i = numMsgs; 0 < i; i--) {
            List<PipeDescriptorImpl> pdList = qRepos.getOldestPopActivePipes(cq.getName(), numMsgs * 2);
            PipeDescriptorImpl pipeDesc = pdList.get(0);
            assertEquals("as messages are commited, should be removed from pending pipe", i, qRepos
                    .getPendingMessagesFromPipe(pipeDesc, numMsgs * 2).size());
            if (0 == i % 2) {
                popper.commit(msgList.remove(0));
            }
            else {
                rollbackList.add(popper.rollback(msgList.remove(0)));
            }
        }

        List<PipeDescriptorImpl> pdList = qRepos.getOldestPopActivePipes(cq.getName(), numMsgs * 2);
        assertEquals("should now be an extra pipe with rollbacks", 2, pdList.size());
        assertEquals("commiting/rollback is complete, shouldn't be any messages in pending pipe", 0, qRepos
                .getPendingMessagesFromPipe(pdList.get(0), numMsgs * 2).size());
        assertEquals("commiting/rollback is complete, should be " + numMsgs / 2 + "messages in waiting pipe",
                numMsgs / 2, qRepos.getWaitingMessagesFromPipe(pdList.get(1), numMsgs * 2).size());

        // need a new popper to select the rollback pipe
        PopperImpl popper2 = cq.createPopper();
        for (int i = 0; i < numMsgs / 2; i++) {
            assertEquals("i = " + i, rollbackList.remove(0), popper2.pop());
        }
    }

    @Test
    public void testPipeDescriptorExpires() throws Exception {
        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 10, 30000, false);
        UUID pipeId1 = UUIDGen.makeType1UUIDFromHost(MyIp.get());
        UUID pipeId2 = UUIDGen.makeType1UUIDFromHost(MyIp.get());
        qRepos.createPipeDescriptor(cq.getName(), pipeId1, 1);
        qRepos.createPipeDescriptor(cq.getName(), pipeId2, System.currentTimeMillis());
        PopperImpl popper = cq.createPopper();
        popper.pop();

        cq.forcePipeReaperWakeUp();
        Thread.sleep(100);

        assertNull(qRepos.getPipeDescriptor(pipeId1));
        assertEquals(PipeStatus.ACTIVE, qRepos.getPipeDescriptor(pipeId2).getPopStatus());
    }

    // -------------------------

    @Before
    public void setupTest() throws Exception {
//        cqFactory = new CassQueueFactoryImpl(qRepos, new HazelcastLockerImpl<QueueDescriptor>("junits"), new HazelcastLockerImpl<QueueDescriptor>("junits"));
        cqFactory = new CassQueueFactoryImpl(qRepos, new LocalLockerImpl<QueueDescriptor>(), new LocalLockerImpl<QueueDescriptor>());
    }
}
