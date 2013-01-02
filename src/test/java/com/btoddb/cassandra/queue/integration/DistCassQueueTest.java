package com.btoddb.cassandra.queue.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.btoddb.cassandra.queue.locks.Locker;
import com.btoddb.cassandra.queue.locks.hazelcast.HazelcastLockerImpl;
import org.junit.Before;
import org.junit.Test;

import com.btoddb.cassandra.queue.CassQMsg;
import com.btoddb.cassandra.queue.CassQueueFactoryImpl;
import com.btoddb.cassandra.queue.CassQueueImpl;
import com.btoddb.cassandra.queue.CassQueueTestBase;
import com.btoddb.cassandra.queue.PopperImpl;
import com.btoddb.cassandra.queue.PusherImpl;
import com.btoddb.cassandra.queue.QueueDescriptor;
import com.btoddb.cassandra.queue.QueueStats;
import com.btoddb.cassandra.queue.app.CassQueueUtils;
import com.btoddb.cassandra.queue.app.PushPopAbstractBase;
import com.btoddb.cassandra.queue.app.QueueProperties;
import com.btoddb.cassandra.queue.app.WorkerThreadWatcher;

/**
 * Tests for {@link CassQueueImpl}.
 * 
 * @author Todd Burruss
 * @author Andrew Ebaugh
 */
public class DistCassQueueTest extends CassQueueTestBase {

    private static CassQueueFactoryImpl cqFactory;
    private static Locker<QueueDescriptor> queueStatsLocker;
    private static Locker<QueueDescriptor> pipeCollectionLocker;

    @Test
    public void testTruncate() throws Exception {
        int maxPushesPerPipe = 2;
        CassQueueImpl cq =
                cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe, 30000, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();

        int numMsgs = 20;
        for (int i = 0; i < numMsgs; i++) {
            pusher.push("xxx_" + i);
        }

        for (int i = 0; i < numMsgs / 2; i++) {
            popper.pop();
        }

        cq.truncate();

        assertEquals("all data should have been truncated", 0, qRepos.getCountOfWaitingMsgs(cq.getName(),
                maxPushesPerPipe).totalMsgCount);
        assertEquals("all data should have been truncated", 0, qRepos.getCountOfPendingCommitMsgs(cq.getName(),
                maxPushesPerPipe).totalMsgCount);

        for (int i = 0; i < numMsgs; i++) {
            CassQMsg qMsg = popper.pop();
            assertNull("should not have found a msg - index = " + i + " : " + (null != qMsg ? qMsg.toString() : null),
                    qMsg);
        }

        cq.shutdownAndWait();

        assertLockerCountsAreCorrect();
    }

    @Test
    public void testSimultaneousSinglePusherSinglePopper() throws Exception {
        assertPushersPoppersWork(1, 1, 100, 1000, 0, 0);
    }

    @Test
    public void testSimultaneousSinglePusherMultiplePoppers() throws Exception {
        assertPushersPoppersWork(1, 4, 100, 2000, 0, 0);
    }

    @Test
    public void testSimultaneousMultiplePushersMultiplePoppers() throws Exception {
        assertPushersPoppersWork(2, 4, 100, 2000, 3, 0);
    }

    @Test
    public void testStats() throws Exception {
        int maxPushesPerPipe = 10;
        int msgCount = maxPushesPerPipe * 4;

        CassQueueImpl cq =
                cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe, 30000, false);

        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();
        for (int i = 0; i < msgCount + 1; i++) {
            pusher.push("1");
        }

        cq.forcePipeReaperWakeUp();
        Thread.sleep(100);

        QueueStats qStats = qRepos.getQueueStats(cq.getName());
        assertEquals(msgCount, qStats.getTotalPushes());
        assertEquals(0, qStats.getTotalPops());

        // popper.forceRefresh();
        for (int i = 0; i < msgCount + 1; i++) {
            assertNotNull("should not be null : i = " + i, popper.pop());
        }

        cq.forcePipeReaperWakeUp();
        Thread.sleep(100);

        qStats = qRepos.getQueueStats(cq.getName());
        assertEquals(msgCount, qStats.getTotalPushes());
        assertEquals(msgCount, qStats.getTotalPops());

        cq.shutdownAndWait();

        assertLockerCountsAreCorrect();
    }

    // -----------------------

    private String assertPushersPoppersWork(int numPushers, int numPoppers, int maxPushesPerPipe, int numMsgs,
            long pushDelay, long popDelay) throws Exception {
        Set<CassQMsg> msgSet = new LinkedHashSet<CassQMsg>();
        Set<String> valueSet = new HashSet<String>();
        Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();

        //
        // start a set of pushers and poppers
        //

        QueueProperties tmpProps = baseEnvProps.clone();
        tmpProps.setNumMsgs(numMsgs);
        tmpProps.setNumPushers(numPushers);
        tmpProps.setPushDelay(pushDelay);
        tmpProps.setNumPoppers(numPoppers);
        tmpProps.setPopDelay(popDelay);

        CassQueueImpl cq =
                cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe, 30000, false);
        cq.setPipeReaperProcessingDelay(100);
        WorkerThreadWatcher pusherWtw = CassQueueUtils.startPushers(cq, tmpProps);
        WorkerThreadWatcher popperWtw = CassQueueUtils.startPoppers(cq, popQ, tmpProps);
        List<PushPopAbstractBase> pusherSet = pusherWtw.getWorkerList();
        List<PushPopAbstractBase> popperSet = popperWtw.getWorkerList();

        boolean finishedProperly = CassQueueUtils.monitorPushersPoppers(popQ, pusherSet, popperSet, msgSet, valueSet);

        assertTrue("monitoring of pushers/poppers finished improperly", finishedProperly);

        assertTrue("expected pusher to be finished", CassQueueUtils.isPushPopOpFinished(pusherSet));
        assertTrue("expected popper to be finished", CassQueueUtils.isPushPopOpFinished(popperSet));

        int totalPushed = 0;
        for (PushPopAbstractBase pusher : pusherSet) {
            totalPushed += pusher.getMsgsProcessed();
        }
        int totalPopped = 0;
        for (PushPopAbstractBase popper : popperSet) {
            totalPopped += popper.getMsgsProcessed();
        }
        assertEquals("did not push the expected number of messages", numMsgs, totalPushed);
        assertEquals("did not pop the expected number of messages", numMsgs, totalPopped);

        assertEquals("expected to have a total of " + numMsgs + " messages in set", numMsgs, msgSet.size());
        assertEquals("expected to have a total of " + numMsgs + " values in set", numMsgs, valueSet.size());

        assertEquals("waiting queue should be empty", 0,
                qRepos.getCountOfWaitingMsgs(cq.getName(), maxPushesPerPipe).totalMsgCount);
        assertEquals("delivered queue should be empty", 0, qRepos.getCountOfPendingCommitMsgs(cq.getName(),
                maxPushesPerPipe).totalMsgCount);

        // wait here for threads to finish up processing before we attempt to
        // interrupt them
        pusherWtw.shutdownAndWait();
        popperWtw.shutdownAndWait();
        cq.shutdownAndWait();

        assertLockerCountsAreCorrect();

        return cq.getName();
    }

    private void assertLockerCountsAreCorrect() {
        System.out.println("Queue stats lock acquire count: " + queueStatsLocker.getLockCountSuccess() + ""
                + ", release count: " + queueStatsLocker.getReleaseCount());

        assertTrue("queueStatsLocker acquire count should equals release; " + "acquire: "
                + queueStatsLocker.getLockCountSuccess() + ", release: " + queueStatsLocker.getReleaseCount(),
                queueStatsLocker.getLockCountSuccess() == queueStatsLocker.getReleaseCount());

    }

    @Before
    public void setupTest() throws Exception {
//        queueStatsLocker = new CagesLockerImpl<QueueDescriptor>("/queue-stats", ZK_CONNECT_STRING, 6000, 30);
//        pipeCollectionLocker = new CagesLockerImpl<QueueDescriptor>("/pipes", ZK_CONNECT_STRING, 6000, 30);

        queueStatsLocker = new HazelcastLockerImpl<QueueDescriptor>("queue-stats");
        pipeCollectionLocker = new HazelcastLockerImpl<QueueDescriptor>("pipes");

        cqFactory = new CassQueueFactoryImpl(qRepos, queueStatsLocker, pipeCollectionLocker);
    }

}
