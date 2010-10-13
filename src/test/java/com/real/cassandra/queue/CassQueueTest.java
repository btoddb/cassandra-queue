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
import com.real.cassandra.queue.app.QueueProperties;
import com.real.cassandra.queue.app.PushPopAbstractBase;
import com.real.cassandra.queue.locks.LocalLockerImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorFactory;

/**
 * Tests for {@link CassQueueImpl}.
 * 
 * @author Todd Burruss
 */
public class CassQueueTest extends CassQueueTestBase {
    // private static Logger logger =
    // LoggerFactory.getLogger(CassQueueTest.class);

    private static CassQueueFactoryImpl cqFactory;

    @Test
    public void testTruncate() throws Exception {
        int maxPushesPerPipe = 2;
        CassQueueImpl cq =
                cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe, 1, 5000, false);
        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper(false);

        int numMsgs = 20;
        for (int i = 0; i < numMsgs; i++) {
            pusher.push("xxx_" + i);
        }

        for (int i = 0; i < numMsgs / 2; i++) {
            popper.pop();
        }

        cq.truncate();

        assertEquals("all data should have been truncated", 0,
                qRepos.getCountOfWaitingMsgs(cq.getName(), maxPushesPerPipe).totalMsgCount);
        assertEquals("all data should have been truncated", 0,
                qRepos.getCountOfPendingCommitMsgs(cq.getName(), maxPushesPerPipe).totalMsgCount);

        for (int i = 0; i < numMsgs; i++) {
            CassQMsg qMsg = popper.pop();
            assertNull("should not have found a msg - index = " + i + " : " + (null != qMsg ? qMsg.toString() : null),
                    qMsg);
        }
    }

    @Test
    public void testSimultaneousSinglePusherSinglePopper() throws Exception {
        assertPushersPoppersWork(1, 1, 100, 1000, 0, 0);
    }

    @Test
    public void testSimultaneousSinglePusherMultiplePoppers() throws Exception {
        assertPushersPoppersWork(1, 4, 100, 10000, 0, 0);
    }

    @Test
    public void testSimultaneousMultiplePushersMultiplePoppers() throws Exception {
        assertPushersPoppersWork(2, 4, 100, 10000, 3, 0);
    }

    @Test
    public void testStats() throws Exception {
        int maxPushesPerPipe = 10;
        int maxPopWidth = 1;
        int msgCount = maxPushesPerPipe * 4;

        CassQueueImpl cq =
                cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe, maxPopWidth,
                        5000, false);

        PusherImpl pusher = cq.createPusher();
        PopperImpl popper = cq.createPopper(false);
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
                cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, maxPushesPerPipe, 4, 5000, false);
        cq.setPipeReaperProcessingDelay(100);
        List<PushPopAbstractBase> pusherSet = CassQueueUtils.startPushers(cq, tmpProps);
        List<PushPopAbstractBase> popperSet = CassQueueUtils.startPoppers(cq, popQ, tmpProps);

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
        assertEquals("delivered queue should be empty", 0,
                qRepos.getCountOfPendingCommitMsgs(cq.getName(), maxPushesPerPipe).totalMsgCount);

        return cq.getName();
    }

    @Before
    public void setupTest() throws Exception {
        cqFactory =
                new CassQueueFactoryImpl(qRepos, new PipeDescriptorFactory(qRepos), new LocalLockerImpl(),
                        new LocalLockerImpl());
    }

}
