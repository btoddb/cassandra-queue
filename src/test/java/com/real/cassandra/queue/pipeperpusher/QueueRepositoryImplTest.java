package com.real.cassandra.queue.pipeperpusher;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Test;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Pelops;

import com.real.cassandra.queue.CassQMsg;

public class QueueRepositoryImplTest extends PipePerPusherTestBase {

    @Test
    public void testInitCassandra() throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(qRepos.getSystemPool().getCluster());
        KsDef ksDef = ksMgr.getKeyspaceSchema(QueueRepositoryImpl.QUEUE_KEYSPACE_NAME);
        List<CfDef> cfList = ksDef.getCf_defs();

        Set<String> nameSet = new HashSet<String>();
        for (CfDef cfDef : cfList) {
            nameSet.add(cfDef.getName());
        }

        assertTrue("didn't create '" + QueueRepositoryImpl.ACTIVE_PIPES_COLFAM + "' column family",
                nameSet.contains(QueueRepositoryImpl.ACTIVE_PIPES_COLFAM));
        assertTrue("didn't create '" + QueueRepositoryImpl.QUEUE_DESCRIPTORS_COLFAM + "' column family",
                nameSet.contains(QueueRepositoryImpl.QUEUE_DESCRIPTORS_COLFAM));
    }

    @Test
    public void testCreateQueueDoesntExist() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe);

        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(qRepos.getSystemPool().getCluster());
        KsDef ksDef = ksMgr.getKeyspaceSchema(QueueRepositoryImpl.QUEUE_KEYSPACE_NAME);
        List<CfDef> cfList = ksDef.getCf_defs();

        Set<String> nameSet = new HashSet<String>();
        for (CfDef cfDef : cfList) {
            nameSet.add(cfDef.getName());
        }

        assertTrue("didn't create '" + QueueRepositoryImpl.formatDeliveredColFamName(qName) + "' column family",
                nameSet.contains(QueueRepositoryImpl.formatDeliveredColFamName(qName)));
        assertTrue("didn't create '" + QueueRepositoryImpl.formatWaitingColFamName(qName) + "' column family",
                nameSet.contains(QueueRepositoryImpl.formatWaitingColFamName(qName)));

        QueueDescriptor qDesc = qRepos.getQueueDescriptor(qName);

        assertEquals(maxPushTimeOfPipe, qDesc.getMaxPushTimeOfPipe());
        assertEquals(maxPushesPerPipe, qDesc.getMaxPushesPerPipe());
    }

    @Test
    public void testCreateQueueDoesExistSameDesc() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe);

        QueueDescriptor qDesc = qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe);

        assertEquals(maxPushTimeOfPipe, qDesc.getMaxPushTimeOfPipe());
        assertEquals(maxPushesPerPipe, qDesc.getMaxPushesPerPipe());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateQueueDoesExistDifferentDesc() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe);

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe * 2, maxPushesPerPipe * 2);
    }

    @Test
    public void testInsert() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe);

        String msgData = "get the msg";

        UUID pipeId = UUIDGen.makeType1UUIDFromHost(inetAddr.get());

        qRepos.setPipeDescriptorActive(qName, pipeId, true);

        UUID msgId = UUIDGen.makeType1UUIDFromHost(inetAddr.get());

        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(qName, pipeId);
        pipeDesc.setMsgCount(1);
        CassQMsg qMsg = qMsgFactory.createInstance(pipeDesc, msgId, msgData);
        qRepos.insert(qName, pipeDesc, msgId, msgData);

        CassQMsg qMsgNew = qRepos.getMsg(qName, pipeId, msgId);

        assertEquals("inserted value is not equal to the retrieved value", qMsg.getMsgData(), qMsgNew.getMsgData());
        assertTrue("pipe descriptor should be active", ((PipeDescriptorImpl) qMsgNew.getPipeDescriptor()).isActive());
        assertEquals("inserted one value, so pipe descriptor msg count should reflect this", 1,
                ((PipeDescriptorImpl) qMsgNew.getPipeDescriptor()).getMsgCount());
    }
}
