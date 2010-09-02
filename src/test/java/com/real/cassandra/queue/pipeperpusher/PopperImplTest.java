package com.real.cassandra.queue.pipeperpusher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.real.cassandra.queue.CassQMsg;

public class PopperImplTest extends PipePerPusherTestBase {
    private CassQueueFactoryImpl cqFactory;

    @Test
    public void testSinglePopper() throws Exception {
        CassQueueImpl cq = cqFactory.createQueueInstance("test_" + System.currentTimeMillis(), 20000, 10, 1, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();

        String msgData = "the data-" + System.currentTimeMillis();
        CassQMsg qMsgPush = pusher.push(msgData);
        CassQMsg qMsgPop = popper.pop();

        assertEquals("msg pushed isn't msg popped", qMsgPush, qMsgPop);
        assertEquals("msg pushed has different value than msg popped", qMsgPush.getMsgData(), qMsgPop.getMsgData());

        List<PipeDescriptorImpl> pipeDescList = qRepos.getOldestNonEmptyPipes(cq.getName(), 1);
        PipeDescriptorImpl pipeDesc = pipeDescList.get(0);

        assertNull("msg should have been moved from waiting to delivered", qRepos.getOldestMsgFromWaitingPipe(pipeDesc));
        CassQMsg qDelivMsg = qRepos.getOldestMsgFromDeliveredPipe(pipeDesc);
        assertEquals("msg pushed isn't msg in delivered pipe", qMsgPush, qDelivMsg);
        assertEquals("msg pushed has different value than msg in delivered pipe", qMsgPush.getMsgData(),
                qDelivMsg.getMsgData());
    }

    @Test
    public void testMultiplePoppersSinglePipeNotThreaded() throws Exception {
        CassQueueImpl cq = cqFactory.createQueueInstance("test_" + System.currentTimeMillis(), 20000, 10, 1, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl[] popperArr = new PopperImpl[] {
                cq.createPopper(), cq.createPopper(), cq.createPopper() };
        int msgCount = 6;
        ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(msgCount);

        for (int i = 0; i < msgCount; i++) {
            msgList.add(pusher.push("data-" + System.currentTimeMillis() + "-" + i));
        }
        for (int i = 0; i < msgCount; i++) {
            CassQMsg qPushMsg = msgList.get(i);
            CassQMsg qPopMsg = popperArr[i % popperArr.length].pop();
            assertEquals("pushed msg not same as popped", qPushMsg, qPopMsg);
            assertEquals("pushed msg data not same as popped data", qPushMsg.getMsgData(), qPopMsg.getMsgData());
        }

        List<PipeDescriptorImpl> pipeDescList = qRepos.getOldestNonEmptyPipes(cq.getName(), 1);
        PipeDescriptorImpl pipeDesc = pipeDescList.get(0);

        assertNull("all msgs should have been moved from waiting to delivered",
                qRepos.getOldestMsgFromWaitingPipe(pipeDesc));

        for (int i = 0; i < msgCount; i++) {
            CassQMsg qPushMsg = msgList.get(i);
            CassQMsg qDelivMsg = qRepos.getOldestMsgFromDeliveredPipe(pipeDesc);
            qRepos.removeMsgFromDeliveredPipe(pipeDesc, qDelivMsg);
            assertEquals("pushed msg not same as popped", qPushMsg, qDelivMsg);
            assertEquals("pushed msg data not same as popped data", qPushMsg.getMsgData(), qDelivMsg.getMsgData());
        }

    }

    @Test
    public void testSinglePopperMultipePipes() throws Exception {
        int maxPushesPerPipe = 5;
        int numPopPipes = 3;
        CassQueueImpl cq =
                cqFactory.createQueueInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe,
                        numPopPipes, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();

        int msgCount = maxPushesPerPipe * numPopPipes * 3;
        CassQMsg[] msgArr = new CassQMsg[msgCount];

        int numPerBlock = maxPushesPerPipe * numPopPipes;
        for (int i = 0; i < msgCount; i++) {
            int baseBlockIndex = i / numPerBlock * numPerBlock;
            int baseRowIndex = (i - baseBlockIndex) / maxPushesPerPipe;
            int index = baseBlockIndex + baseRowIndex + i % maxPushesPerPipe * numPopPipes;
            msgArr[index] = pusher.push("data-" + System.currentTimeMillis() + "-" + i);
        }

        for (int i = 0; i < msgCount; i++) {
            CassQMsg qPushMsg = msgArr[i];
            CassQMsg qPopMsg = popper.pop();
            assertEquals("pushed msg not same as popped", qPushMsg, qPopMsg);
            assertEquals("pushed msg data not same as popped data", qPushMsg.getMsgData(), qPopMsg.getMsgData());
        }

        // List<PipeDescriptorImpl> pipeDescList =
        // qRepos.getOldestNonEmptyPipes(cq.getName(), 1);
        // PipeDescriptorImpl pipeDesc = pipeDescList.get(0);
        //
        // assertNull("all msgs should have been moved from waiting to delivered",
        // qRepos.getOldestMsgFromWaitingPipe(pipeDesc));
        //
        // for (int i = 0; i < msgCount; i++) {
        // CassQMsg qPushMsg = msgList.get(i);
        // CassQMsg qDelivMsg = qRepos.getOldestMsgFromDeliveredPipe(pipeDesc);
        // qRepos.removeMsgFromDeliveredPipe(pipeDesc, qDelivMsg);
        // assertEquals("pushed msg not same as popped", qPushMsg, qDelivMsg);
        // assertEquals("pushed msg data not same as popped data",
        // qPushMsg.getMsgData(), qDelivMsg.getMsgData());
        // }

    }

    @Test
    public void testMarkingPipeAsFinishedEmpty() throws Exception {
        CassQueueImpl cq = cqFactory.createQueueInstance("test_" + System.currentTimeMillis(), 20000, 10, 1, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();
        int msgCount = 10;

        for (int i = 0; i < msgCount; i++) {
            pusher.push("data-" + System.currentTimeMillis() + "-" + i);
        }
        pusher.push("over-to-next");
        List<PipeDescriptorImpl> pipeDescList = qRepos.getOldestNonEmptyPipes(cq.getName(), 1);
        PipeDescriptorImpl pipeDesc = pipeDescList.get(0);
        qRepos.setPipeDescriptorStatus(cq.getName(), pipeDesc, PipeDescriptorImpl.STATUS_PUSH_FINISHED);

        for (int i = 0; i < msgCount; i++) {
            popper.pop();
            pipeDesc = qRepos.getPipeDescriptor(cq.getName(), pipeDesc.getPipeId());
            assertEquals("pipe should still be " + PipeDescriptorImpl.STATUS_PUSH_FINISHED,
                    PipeDescriptorImpl.STATUS_PUSH_FINISHED, pipeDesc.getStatus());
        }

        // do one more pop to push over the edge to next pipe
        assertEquals("should have rolled to next pipe and retrieved next msg", "over-to-next", popper.pop()
                .getMsgData());

        pipeDesc = qRepos.getPipeDescriptor(cq.getName(), pipeDesc.getPipeId());
        assertEquals("pipe should now be " + PipeDescriptorImpl.STATUS_FINISHED_AND_EMPTY,
                PipeDescriptorImpl.STATUS_FINISHED_AND_EMPTY, pipeDesc.getStatus());

    }

    @Test
    public void testNoPipes() throws Exception {
        CassQueueImpl cq = cqFactory.createQueueInstance("test_" + System.currentTimeMillis(), 20000, 10, 1, false);
        PopperImpl popper = cq.createPopper();
        assertNull("should return null", popper.pop());
    }

    @Test
    public void testShutdownInProgress() throws Exception {
        CassQueueImpl cq = cqFactory.createQueueInstance("test_" + System.currentTimeMillis(), 20000, 10, 1, false);
        PopperImpl popper = cq.createPopper();
        PusherImpl pusher = cq.createPusher();

        pusher.push("blah1");
        pusher.push("blah1");
        pusher.push("blah1");

        popper.pop();

        popper.shutdown();

        try {
            popper.pop();
            fail("pusher should not have allowed a new message since in shutdown mode");
        }
        catch (IllegalStateException e) {
            // all good, expected
        }
    }

    // -------------------------

    @Before
    public void setupTest() throws Exception {
        cqFactory = new CassQueueFactoryImpl(qRepos, new PipeDescriptorFactory());
    }
}
