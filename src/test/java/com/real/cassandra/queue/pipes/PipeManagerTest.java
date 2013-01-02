package com.real.cassandra.queue.pipes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import com.real.cassandra.queue.utils.UuidGenerator;
import org.junit.Before;
import org.junit.Test;

import com.real.cassandra.queue.CassQueueException;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.CassQueueTestBase;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.locks.LocalLockerImpl;

public class PipeManagerTest extends CassQueueTestBase {
    CassQueueImpl cq;
    LocalLockerImpl<QueueDescriptor> pipeCollectionLocker;

    @Test
    public void testPickPipe() {
        PipeDescriptorImpl pd1 = qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());
        PipeDescriptorImpl pd2 = qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());
        PipeDescriptorImpl pd3 = qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());

        System.out.println("pd1 = " + pd1.getId());
        System.out.println("pd2 = " + pd2.getId());
        System.out.println("pd3 = " + pd3.getId());

        PipeManager pipeMgr1 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        assertEquals(pd1, pipeMgr1.pickPipe());
        PipeManager pipeMgr2 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        assertEquals(pd2, pipeMgr2.pickPipe());
        PipeManager pipeMgr3 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        assertEquals(pd3, pipeMgr3.pickPipe());
    }

    @Test
    public void testNoneAvailable() {
        PipeDescriptorImpl pd1 = qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());
        PipeDescriptorImpl pd2 = qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());
        PipeDescriptorImpl pd3 = qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());

        PipeManager pipeMgr1 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        assertEquals(pd1, pipeMgr1.pickPipe());
        PipeManager pipeMgr2 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        assertEquals(pd2, pipeMgr2.pickPipe());
        PipeManager pipeMgr3 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        assertEquals(pd3, pipeMgr3.pickPipe());

        // this one shouldn't find an available pipe
        PipeManager pipeMgr4 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        assertNull( "should not have returned a pipe, because all are used", pipeMgr4.pickPipe() );
    }

    @Test
    public void testPickPipeSameOwner() {
        PipeDescriptorImpl pd1 = qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());
        PipeDescriptorImpl pd2 = qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());
        qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());

        PipeManager pipeMgr1 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        assertEquals(pd1, pipeMgr1.pickPipe());
        PipeManager pipeMgr2 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        assertEquals(pd2, pipeMgr2.pickPipe());
        assertEquals(pd2, pipeMgr2.pickPipe());
    }

    @Test
    public void testPickPipeExpired() throws Exception {
        PipeDescriptorImpl pd1 = qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());

        PipeManager pipeMgr1 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        assertEquals(pd1, pipeMgr1.pickPipe());
        PipeManager pipeMgr2 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);
        try {
            pipeMgr2.pickPipe();
        }
        catch (CassQueueException e) {
            // yay!
        }

        cq.setTransactionTimeout(200);
        pipeMgr2.setMaxOwnerIdleTime(200);
        Thread.sleep(1500);
        assertEquals(pd1, pipeMgr2.pickPipe());
    }

    @Test
    public void testMarkPushFinished() throws Exception {
        PipeDescriptorImpl pd1 = qRepos.createPipeDescriptor(cq.getName(), UuidGenerator.generateTimeUuid());

        cq.setMaxPushTimePerPipe(1);
        PipeManager pipeMgr1 = new PipeManager(qRepos, cq, UUID.randomUUID(), pipeCollectionLocker);

        // wait until grace is over
        Thread.sleep(PipeManager.GRACE_EXTRA_EXPIRE_TIME);
        
        // pick should select the expired pipe, because don't know if any pop's
        // left in it
        assertNotNull(pipeMgr1.pickPipe());

        assertTrue( pipeMgr1.checkMarkPopFinished(pd1));
    }

    // ---------------

    @Before
    public void setup() {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 60000;
        int maxPushesPerPipe = 23;

        pipeCollectionLocker = new LocalLockerImpl<QueueDescriptor>();

        QueueDescriptor qd =
                qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);
        cq = new CassQueueImpl(qRepos, qd, false, new LocalLockerImpl<QueueDescriptor>(), pipeCollectionLocker);
    }
}
