package com.real.cassandra.queue.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import me.prettyprint.hector.api.ddl.HCfDef;
import me.prettyprint.hector.api.ddl.HKsDef;

import org.apache.cassandra.utils.UUIDGen;
import org.junit.Test;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueTestBase;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueStats;
import com.real.cassandra.queue.QueueStatsFactoryImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.utils.MyIp;
import com.real.cassandra.queue.utils.UuidGenerator;

public class QueueRepositoryImplTest extends CassQueueTestBase {
    private QueueStatsFactoryImpl qStatsFactory = new QueueStatsFactoryImpl();

    @Test
    public void testInitCassandra() throws Exception {
        HKsDef ksDef = qRepos.getKeyspaceDefinition();

        Set<String> nameSet = new HashSet<String>();
        for (HCfDef cfDef : ksDef.getCfDefs()) {
            nameSet.add(cfDef.getName());
        }

        assertTrue("didn't create '" + QueueRepositoryImpl.QUEUE_DESCRIPTORS_COLFAM + "' column family",
                nameSet.contains(QueueRepositoryImpl.QUEUE_DESCRIPTORS_COLFAM));
        assertTrue("didn't create '" + QueueRepositoryImpl.PIPE_DESCRIPTOR_COLFAM + "' column family",
                nameSet.contains(QueueRepositoryImpl.PIPE_DESCRIPTOR_COLFAM));
        assertTrue("didn't create '" + QueueRepositoryImpl.QUEUE_PIPE_CNXN_COLFAM + "' column family",
                nameSet.contains(QueueRepositoryImpl.QUEUE_PIPE_CNXN_COLFAM));
    }

    @Test
    public void testCreateQueueDoesntExist() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        int maxPopWidth = 4;
        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);

        HKsDef ksDef = qRepos.getKeyspaceDefinition();
        List<HCfDef> cfList = ksDef.getCfDefs();

        Set<String> nameSet = new HashSet<String>();
        for (HCfDef cfDef : cfList) {
            nameSet.add(cfDef.getName());
        }

        assertTrue("didn't create '" + QueueRepositoryImpl.formatPendingColFamName(qName) + "' column family",
                nameSet.contains(QueueRepositoryImpl.formatPendingColFamName(qName)));
        assertTrue("didn't create '" + QueueRepositoryImpl.formatWaitingColFamName(qName) + "' column family",
                nameSet.contains(QueueRepositoryImpl.formatWaitingColFamName(qName)));

        QueueDescriptor qDesc = qRepos.getQueueDescriptor(qName);

        assertEquals(maxPushTimeOfPipe, qDesc.getMaxPushTimeOfPipe());
        assertEquals(maxPushesPerPipe, qDesc.getMaxPushesPerPipe());
        assertEquals(maxPopWidth, qDesc.getMaxPopWidth());
    }

    @Test
    public void testCreateQueueExistsSameDesc() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        int maxPopWidth = 4;
        long popPipeRefreshDelay = 1000;
        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, popPipeRefreshDelay);

        QueueDescriptor qDesc =
                qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);

        assertEquals(maxPushTimeOfPipe, qDesc.getMaxPushTimeOfPipe());
        assertEquals(maxPushesPerPipe, qDesc.getMaxPushesPerPipe());
        assertEquals(maxPopWidth, qDesc.getMaxPopWidth());
        assertEquals(popPipeRefreshDelay, qDesc.getPopPipeRefreshDelay());
    }

    @Test
    public void testCreateQueueDoesExistDifferentDesc() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        int maxPopWidth = 4;
        long popPipeRefreshDelay = 1000;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, popPipeRefreshDelay);

        QueueDescriptor qNewDesc =
                qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe * 2, maxPushesPerPipe * 2, maxPopWidth * 2,
                        popPipeRefreshDelay * 2);
        assertEquals(maxPushTimeOfPipe, qNewDesc.getMaxPushTimeOfPipe());
        assertEquals(maxPushesPerPipe, qNewDesc.getMaxPushesPerPipe());
        assertEquals(maxPopWidth, qNewDesc.getMaxPopWidth());
        assertEquals(popPipeRefreshDelay, qNewDesc.getPopPipeRefreshDelay());
    }

    @Test
    public void testInsert() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        int maxPopWidth = 4;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);
        PipeDescriptorImpl pipeDesc = qRepos.createPipeDescriptor(qName, UUIDGen.makeType1UUIDFromHost(MyIp.get()));

        UUID msgId = UuidGenerator.generateTimeUuid();
        String msgData = "get the msg";

        pipeDesc.setPushCount(1);
        CassQMsg qMsg = qMsgFactory.createInstance(pipeDesc, msgId, msgData);
        qRepos.insertMsg(pipeDesc, msgId, msgData);

        CassQMsg qMsgNew = qRepos.getMsg(qName, pipeDesc, msgId);

        assertEquals("inserted value is not equal to the retrieved value", qMsg.getMsgData(), qMsgNew.getMsgData());

        PipeDescriptorImpl pdNew = qRepos.getPipeDescriptor(pipeDesc.getPipeId());
        assertTrue("pipe descriptor should be active", pdNew.isPushActive());
        assertEquals("inserted one value, so pipe descriptor msg count should reflect this", 1, pdNew.getPushCount());
    }

    @Test
    public void testCreateUpdateStats() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        int maxPopWidth = 4;
        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);

        QueueStats qs1 = qStatsFactory.createInstance(qName, 123, 456, 1.23, 4.56);
        qRepos.updateQueueStats(qs1);
        QueueStats qs2 = qRepos.getQueueStats(qName);

        assertEquals(qs1.getQName(), qs2.getQName());
        assertEquals(qs1.getRecentPopsPerSec(), qs2.getRecentPopsPerSec(), 0);
        assertEquals(qs1.getRecentPushesPerSec(), qs2.getRecentPushesPerSec(), 0);
        assertEquals(qs1.getTotalPops(), qs2.getTotalPops());
        assertEquals(qs1.getTotalPushes(), qs2.getTotalPushes());
    }

    @Test
    public void testGetOldestMsgFromPipe() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 20;
        int maxPopWidth = 4;
        int msgCount = 15;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);

        UUID pipeId = UuidGenerator.generateTimeUuid();
        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(qName, pipeId);
        qRepos.updatePipePushStatus(pipeDesc, PipeStatus.ACTIVE);
        qRepos.updatePipePopStatus(pipeDesc, PipeStatus.ACTIVE);
        for (int i = 0; i < msgCount; i++) {
            String msgData = "data-" + i;
            UUID msgId = UuidGenerator.generateTimeUuid();
            qRepos.insertMsg(pipeDesc, msgId, msgData);
        }

        // ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(msgCount);
        CassQMsg qMsg;
        int i = 0;
        while (null != (qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc))) {
            assertEquals("data was not retrieved (or inserted) in the proper order, or too much data found", "data-"
                    + i, qMsg.getMsgData());
            qRepos.removeMsgFromWaitingPipe(qMsg);
            i++;
        }
        assertEquals("should have retrieve exactly " + msgCount + " msgs", msgCount, i);
    }

    @Test
    public void testMoveMsgFromWaitingToDeliveredPipe() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 10;
        int maxPopWidth = 4;
        int msgCount = 15;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);

        UUID pipeId = UuidGenerator.generateTimeUuid();
        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(qName, pipeId);
        qRepos.updatePipePushStatus(pipeDesc, PipeStatus.ACTIVE);
        qRepos.updatePipePopStatus(pipeDesc, PipeStatus.ACTIVE);
        for (int i = 0; i < msgCount; i++) {
            String msgData = "data-" + i;
            UUID msgId = UuidGenerator.generateTimeUuid();
            qRepos.insertMsg(pipeDesc, msgId, msgData);
        }

        // ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(msgCount);
        CassQMsg qMsg;
        while (null != (qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc))) {
            qRepos.moveMsgFromWaitingToPendingPipe(qMsg);
        }

        int i = 0;
        while (null != (qMsg = qRepos.getOldestMsgFromDeliveredPipe(pipeDesc))) {
            assertEquals("data was not retrieved (or inserted) in the proper order, or too much data found", "data-"
                    + i, qMsg.getMsgData());
            qRepos.removeMsgFromPendingPipe(qMsg);
            i++;
        }
        assertEquals("should have retrieve exactly " + msgCount + " msgs", msgCount, i);
    }

    @Test
    public void testGetAllNonEmptyPipesInOrder() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 10;
        int maxPopWidth = 4;
        int pipeCount = 20;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);

        ArrayList<UUID> pipeList = new ArrayList<UUID>();
        for (int i = 0; i < pipeCount; i++) {
            UUID pipeId = UuidGenerator.generateTimeUuid();
            PipeDescriptorImpl pipeDesc = qRepos.createPipeDescriptor(qName, pipeId);
            PipeStatus status = 0 == i % 2 ? PipeStatus.ACTIVE : PipeStatus.COMPLETED;
            qRepos.updatePipePushStatus(pipeDesc, status);
            qRepos.updatePipePopStatus(pipeDesc, status);
            pipeList.add(pipeId);
        }

        List<PipeDescriptorImpl> pipeListNew = qRepos.getOldestPopActivePipes(qName, pipeCount / 2);

        assertEquals("should have returned same number of pipes as created", pipeList.size() / 2, pipeListNew.size());

        Iterator<PipeDescriptorImpl> newIter = pipeListNew.iterator();
        for (int i = 0; i < pipeCount; i++) {
            if (0 == i % 2) {
                UUID pipeTargetId = pipeList.get(i);
                PipeDescriptorImpl pipeDesc = newIter.next();
                assertEquals("pipe descriptors are not returned in expected order", pipeTargetId, pipeDesc.getPipeId());
            }
        }
    }

    @Test
    public void testGetAllNonEmptyPipesAllEmpty() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 10;
        int maxPopWidth = 4;
        int pipeCount = 5;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);

        ArrayList<UUID> pipeList = new ArrayList<UUID>();
        for (int i = 0; i < pipeCount; i++) {
            UUID pipeId = UuidGenerator.generateTimeUuid();
            PipeDescriptorImpl pipeDesc = qRepos.createPipeDescriptor(qName, pipeId);
            PipeStatus status = PipeStatus.COMPLETED;
            qRepos.updatePipePushStatus(pipeDesc, status);
            qRepos.updatePipePopStatus(pipeDesc, status);
            pipeList.add(pipeId);
        }

        List<PipeDescriptorImpl> pipeListNew = qRepos.getOldestPopActivePipes(qName, pipeCount + 1);

        assertEquals("all pipes are finished and empty, should not have returned any", 0, pipeListNew.size());
    }
}
