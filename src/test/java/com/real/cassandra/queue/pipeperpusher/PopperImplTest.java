package com.real.cassandra.queue.pipeperpusher;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.real.cassandra.queue.CassQMsg;

public class PopperImplTest extends PipePerPusherTestBase {
    private CassQueueImpl cq;
    private CassQueueFactoryImpl cqFactory;

    @Test
    public void testSinglePopperSingleMsg() throws Exception {
        PusherImpl pusher = cqFactory.createPusher(cq);
        PopperImpl popper = cqFactory.createPopper(cq);

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

    // -------------------------

    @Before
    public void setupTest() throws Exception {
        cqFactory = new CassQueueFactoryImpl(qRepos, new PipeDescriptorFactory());
        cq = cqFactory.createQueueInstance("test_" + System.currentTimeMillis(), 20000, 10, 1, false);
    }
}
