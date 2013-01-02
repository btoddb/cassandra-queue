package com.btoddb.cassandra.queue.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import org.junit.Test;

import com.btoddb.cassandra.queue.CassQMsg;
import com.btoddb.cassandra.queue.CassQueueTestBase;
import com.btoddb.cassandra.queue.QueueDescriptor;
import com.btoddb.cassandra.queue.QueueStats;
import com.btoddb.cassandra.queue.pipes.PipeDescriptorImpl;
import com.btoddb.cassandra.queue.pipes.PipeStatus;
import com.btoddb.cassandra.queue.utils.UuidGenerator;

public class QueueRepositoryImplTest extends CassQueueTestBase {

    @Test
    public void testInitCassandra() throws Exception {
        KeyspaceDefinition ksDef = qRepos.getKeyspaceDefinition();

        Set<String> nameSet = new HashSet<String>();
        for (ColumnFamilyDefinition cfDef : ksDef.getCfDefs()) {
            nameSet.add(cfDef.getName());
        }

        assertTrue("didn't create '" + QueueRepositoryImpl.QUEUE_DESCRIPTORS_COLFAM + "' column family", nameSet
                .contains(QueueRepositoryImpl.QUEUE_DESCRIPTORS_COLFAM));
        assertTrue("didn't create '" + QueueRepositoryImpl.PIPE_DESCRIPTOR_COLFAM + "' column family", nameSet
                .contains(QueueRepositoryImpl.PIPE_DESCRIPTOR_COLFAM));
        assertTrue("didn't create '" + QueueRepositoryImpl.QUEUE_PIPE_CNXN_COLFAM + "' column family", nameSet
                .contains(QueueRepositoryImpl.QUEUE_PIPE_CNXN_COLFAM));
    }
    
    @Test
    public void testCreateQueueDoesntExist() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        QueueDescriptor qd1 = qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);

        KeyspaceDefinition ksDef = qRepos.getKeyspaceDefinition();
        List<ColumnFamilyDefinition> cfList = ksDef.getCfDefs();

        Set<String> nameSet = new HashSet<String>();
        for (ColumnFamilyDefinition cfDef : cfList) {
            nameSet.add(cfDef.getName());
        }

        assertTrue("didn't create '" + QueueRepositoryImpl.formatPendingColFamName(qName) + "' column family", nameSet
                .contains(QueueRepositoryImpl.formatPendingColFamName(qName)));
        assertTrue("didn't create '" + QueueRepositoryImpl.formatWaitingColFamName(qName) + "' column family", nameSet
                .contains(QueueRepositoryImpl.formatWaitingColFamName(qName)));

        QueueDescriptor qd2 = qRepos.getQueueDescriptor(qName);

        assertEquals( qd1.getId(), qd2.getId());
        assertEquals(maxPushTimeOfPipe, qd2.getMaxPushTimePerPipe());
        assertEquals(maxPushesPerPipe, qd2.getMaxPushesPerPipe());
    }

    @Test
    public void testCreateQueueExistsSameDesc() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);

        QueueDescriptor qDesc = qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);

        assertEquals(maxPushTimeOfPipe, qDesc.getMaxPushTimePerPipe());
        assertEquals(maxPushesPerPipe, qDesc.getMaxPushesPerPipe());
    }

    @Test
    public void testCreateQueueDoesExistDifferentDesc() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);

        QueueDescriptor qNewDesc = qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe * 2, maxPushesPerPipe * 2, 30000);
        assertEquals(maxPushTimeOfPipe, qNewDesc.getMaxPushTimePerPipe());
        assertEquals(maxPushesPerPipe, qNewDesc.getMaxPushesPerPipe());
    }

    @Test
    public void testInsert() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);
        PipeDescriptorImpl pipeDesc = qRepos.createPipeDescriptor(qName, UuidGenerator.generateTimeUuid());

        UUID msgId = UuidGenerator.generateTimeUuid();
        String msgData = "get the msg";

        pipeDesc.setPushCount(1);
        CassQMsg qMsg = qRepos.insertMsg(pipeDesc, msgId, msgData.getBytes());

        CassQMsg qMsgNew = qRepos.getMsg(qName, pipeDesc, msgId);

        assertEquals("inserted value is not equal to the retrieved value", qMsg.getMsgDesc().getPayloadAsByteBuffer(),
                qMsgNew.getMsgDesc().getPayloadAsByteBuffer());

        PipeDescriptorImpl pdNew = qRepos.getPipeDescriptor(pipeDesc.getPipeId());
        assertTrue("pipe descriptor should be active", pdNew.isPushActive());
        assertEquals("inserted one value, so pipe descriptor msg count should reflect this", 1, pdNew.getPushCount());
    }

    @Test
    public void testCreateUpdateStats() throws Exception {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;
        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);

        QueueStats qs1 = new QueueStats(qName);
        qs1.setRecentPopsPerSec(1.23);
        qs1.setRecentPushesPerSec(4.56);
        qs1.setTotalPops(123);
        qs1.setTotalPushes(456);
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
        int msgCount = 15;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);

        UUID pipeId = UuidGenerator.generateTimeUuid();
        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(qName, pipeId);
        qRepos.updatePipePushStatus(pipeDesc, PipeStatus.ACTIVE);
        qRepos.updatePipePopStatus(pipeDesc, PipeStatus.ACTIVE);
        for (int i = 0; i < msgCount; i++) {
            String msgData = "data-" + i;
            UUID msgId = UuidGenerator.generateTimeUuid();
            qRepos.insertMsg(pipeDesc, msgId, msgData.getBytes());
        }

        // ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(msgCount);
        CassQMsg qMsg;
        int i = 0;
        while (null != (qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc))) {
            assertEquals("data was not retrieved (or inserted) in the proper order, or too much data found", "data-"
                    + i, new String(qMsg.getMsgDesc().getPayload()));
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
        int msgCount = 15;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);

        UUID pipeId = UuidGenerator.generateTimeUuid();
        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(qName, pipeId);
        qRepos.updatePipePushStatus(pipeDesc, PipeStatus.ACTIVE);
        qRepos.updatePipePopStatus(pipeDesc, PipeStatus.ACTIVE);
        for (int i = 0; i < msgCount; i++) {
            String msgData = "data-" + i;
            UUID msgId = UuidGenerator.generateTimeUuid();
            qRepos.insertMsg(pipeDesc, msgId, msgData.getBytes());
        }

        // ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(msgCount);
        CassQMsg qMsg;
        while (null != (qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc))) {
            qRepos.moveMsgFromWaitingToPendingPipe(qMsg);
        }

        int i = 0;
        while (null != (qMsg = qRepos.getOldestMsgFromPendingPipe(pipeDesc))) {
            assertEquals("data was not retrieved (or inserted) in the proper order, or too much data found", "data-"
                    + i, new String(qMsg.getMsgDesc().getPayload()));
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
        int pipeCount = 20;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);

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
        int pipeCount = 5;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);

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

    @Test
    public void testSavePopperOwner() {
        String qName = "test_" + System.currentTimeMillis();
        long maxPushTimeOfPipe = 20000;
        int maxPushesPerPipe = 23;

        qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, 30000);
        PipeDescriptorImpl pd = qRepos.createPipeDescriptor(qName, UuidGenerator.generateTimeUuid());

        assertNull("pop owner should be null at start of pipe", pd.getPopOwner());
        assertNull("pop owner timestamp should be null at start of pipe", pd.getPopOwnTimestamp());

        UUID popOwner = UUID.randomUUID();
        Long now = System.currentTimeMillis();
        qRepos.savePipePopOwner(pd, popOwner, now);

        PipeDescriptorImpl pd2 = qRepos.getPipeDescriptor(pd.getId());

        assertEquals(popOwner, pd2.getPopOwner());
        assertEquals(now, pd2.getPopOwnTimestamp());
    }
}
