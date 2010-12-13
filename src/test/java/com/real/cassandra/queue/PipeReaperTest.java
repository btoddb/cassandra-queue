package com.real.cassandra.queue;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.real.cassandra.queue.locks.LocalLockerImpl;

public class PipeReaperTest extends CassQueueTestBase {
    private CassQueueFactoryImpl cqFactory;

    @Test
    public void testRollbackExpiredPoppedMsgs() throws Exception {
        long transactionTimeout = 1;
        CassQueueImpl cq =
                cqFactory.createInstance("test_" + System.currentTimeMillis(), 60000, 10, transactionTimeout, false);
        PipeReaper reaper = cq.getPipeReaper();
        reaper.shutdownAndWait();
        
        PusherImpl pusher = cq.createPusher();
        pusher.push("data1");
        pusher.push("data2");
        
        PopperImpl popper1 = cq.createPopper();
        
        // make sure doesn't rollback everything
        CassQMsg qMsg1 = popper1.pop();
        assertNotNull( qMsg1 );
        popper1.commit(qMsg1);
        
        // wait for rollback on this one
        qMsg1 = popper1.pop();
        assertNotNull(qMsg1);
        Thread.sleep(CassQueueImpl.TRANSACTION_GRACE_PERIOD); // must wait past grace
        reaper.rollbackExpiredPoppedMsgs();
        
        CassQMsg qMsg2 = cq.createPopper().pop();
        assertNotNull(qMsg2);
        
        assertEquals( qMsg1.getMsgDesc().getPayloadAsByteBuffer(), qMsg2.getMsgDesc().getPayloadAsByteBuffer() );
    }

    // ----------------------

    @Before
    public void setupTest() throws Exception {
        cqFactory =
                new CassQueueFactoryImpl(qRepos, new LocalLockerImpl<QueueDescriptor>(),
                        new LocalLockerImpl<QueueDescriptor>());
    }

}
