package com.real.cassandra.queue;

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

import org.junit.Before;
import org.junit.Test;

import com.real.cassandra.queue.app.CassQueueUtils;
import com.real.cassandra.queue.app.PushPopAbstractBase;
import com.real.cassandra.queue.app.QueueProperties;
import com.real.cassandra.queue.app.WorkerThreadWatcher;
import com.real.cassandra.queue.locks.LocalLockerImpl;

/**
 * Tests for {@link CassQueueImpl}.
 * 
 * @author Todd Burruss
 */
public class CassQueueTest extends CassQueueTestBase {
    // private static Logger logger =
    // LoggerFactory.getLogger(CassQueueTest.class);

    private static CassQueueFactoryImpl cqFactory;
    private static LocalLockerImpl<QueueDescriptor> queueStatsLocker;
    private static LocalLockerImpl<QueueDescriptor> pipeCollectionLocker;

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
    public void testSimultaneousMultiplePushersMultiplePoppers() throws Exception {
        assertPushersPoppersWork(5, 10, 100, 10000, 2, 0);
    }

    @Test
    public void testRollupStatsOnly() throws Exception {
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

        Set<CassQMsg> popSet = new HashSet<CassQMsg>();
        for (int i = 0; i < msgCount + 1; i++) {
            CassQMsg qMsg = popper.pop();
            assertNotNull("should not be null : i = " + i, qMsg);
            popSet.add(qMsg);
        }

        cq.forcePipeReaperWakeUp();
        Thread.sleep(200);

        qStats = qRepos.getQueueStats(cq.getName());
        assertEquals(msgCount, qStats.getTotalPushes());
        assertEquals(msgCount, qStats.getTotalPops());

        for (CassQMsg qMsg : popSet) {
            popper.commit(qMsg);
        }

        cq.forcePipeReaperWakeUp();
        Thread.sleep(200);

        qStats = qRepos.getQueueStats(cq.getName());
        assertEquals(msgCount, qStats.getTotalPushes());
        assertEquals(msgCount, qStats.getTotalPops());

        cq.shutdownAndWait();
        assertLockerCountsAreCorrect();
    }

    @Test
    public void testCurrentStats() throws Exception {
        int maxPushesPerPipe = 10;
        int msgCount = maxPushesPerPipe * 4 + maxPushesPerPipe/2;

        CassQueueImpl cq =
                cqFactory.createInstance("test_" + System.currentTimeMillis(), 1000, maxPushesPerPipe, 30000, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper();
        
        for (int i = 0; i < msgCount; i++) {
            pusher.push("1");
        }

        QueueStats qs1 = qRepos.calculateUpToDateQueueStats(cq.getName());
        assertEquals(msgCount, qs1.getTotalPushes());
        assertEquals(0, qs1.getTotalPops());
        
        for (int i = 0; i < msgCount; i++) {
            popper.pop();
        }

        qs1 = qRepos.calculateUpToDateQueueStats(cq.getName());
        assertEquals(msgCount, qs1.getTotalPushes());
        assertEquals(msgCount, qs1.getTotalPops());

        cq.forcePipeReaperWakeUp();
        Thread.sleep(200);

        QueueStats qs2 = qRepos.getQueueStats(cq.getName());
        assertEquals(msgCount - maxPushesPerPipe/2, qs2.getTotalPushes());
        assertEquals(msgCount - maxPushesPerPipe/2, qs2.getTotalPops());
        
        Thread.sleep( 2000 ); // must acct for the grace period too
        popper.pop(); // one more pop to force the completion of an expired pipe
        cq.forcePipeReaperWakeUp();
        Thread.sleep(200);

        qs2 = qRepos.getQueueStats(cq.getName());
        assertEquals(msgCount, qs2.getTotalPushes());
        assertEquals(msgCount, qs2.getTotalPops());
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

        assertTrue("queueStatsLocker should be empty after shutdown, but has " + queueStatsLocker.getMap().size()
                + " locks still active(" + queueStatsLocker.getLockCountSuccess() + ", " + queueStatsLocker.getReleaseCount()
                + ")", queueStatsLocker.getMap().isEmpty());
    }

    @Before
    public void setupTest() throws Exception {
        queueStatsLocker = new LocalLockerImpl<QueueDescriptor>();
        pipeCollectionLocker = new LocalLockerImpl<QueueDescriptor>();
        cqFactory = new CassQueueFactoryImpl(qRepos, queueStatsLocker, pipeCollectionLocker);
    }

}
