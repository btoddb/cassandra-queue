package com.real.cassandra.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Before;
import org.junit.Test;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueFactoryImpl;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.PopperImpl;
import com.real.cassandra.queue.PusherImpl;
import com.real.cassandra.queue.app.CassQueueUtils;
import com.real.cassandra.queue.app.EnvProperties;
import com.real.cassandra.queue.app.PushPopAbstractBase;
import com.real.cassandra.queue.pipes.PipeDescriptorFactory;
import com.real.cassandra.queue.pipes.PipeLockerImpl;

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
        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 2, 1, 5000, false);
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

        assertEquals("all data should have been truncated", 0, qRepos.getCountOfWaitingMsgs(cq.getName()).totalMsgCount);
        assertEquals("all data should have been truncated", 0,
                qRepos.getCountOfPendingCommitMsgs(cq.getName()).totalMsgCount);

        for (int i = 0; i < numMsgs; i++) {
            CassQMsg qMsg = popper.pop();
            assertNull("should not have found a msg - index = " + i + " : " + (null != qMsg ? qMsg.toString() : null),
                    qMsg);
        }
    }

    @Test
    public void testSimultaneousSinglePusherSinglePopper() throws Exception {
        assertPushersPoppersWork(1, 1, 1000, 0, 0);
    }

    @Test
    public void testSimultaneousSinglePusherMultiplePoppers() throws Exception {
        assertPushersPoppersWork(1, 4, 10000, 0, 0);
    }

    @Test
    public void testSimultaneousMultiplePushersMultiplePoppers() throws Exception {
        assertPushersPoppersWork(2, 4, 10000, 3, 0);
    }

    @Test
    public void testFinishedPipeRemoval() {
        fail("functionality not implemented yet");
    }

    //
    // @Test
    // public void testSimultaneousMultiplePusherMultiplePopper() {
    // cq.setNearFifoOk(true);
    // assertPushersPoppersWork(4, 10, 10000, 4000, 2, 0);
    // }
    //
    // @Test
    // public void testShutdown() {
    // cq.shutdown();
    // }
    //
    // @Test
    // public void testStartStopOfQueue() {
    // fail("not implemented");
    // }

    // -----------------------

    private void assertPushersPoppersWork(int numPushers, int numPoppers, int numMsgs, long pushDelay, long popDelay)
            throws Exception {
        Set<CassQMsg> msgSet = new LinkedHashSet<CassQMsg>();
        Set<String> valueSet = new HashSet<String>();
        Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();

        //
        // start a set of pushers and poppers
        //

        EnvProperties tmpProps = baseEnvProps.clone();
        tmpProps.setNumMsgs(numMsgs);
        tmpProps.setNumPushers(numPushers);
        tmpProps.setPushDelay(pushDelay);
        // tmpProps.setNumMsgsPerPusher(numToPushPerPusher);
        tmpProps.setNumPoppers(numPoppers);
        tmpProps.setPopDelay(popDelay);
        // tmpProps.setNumMsgsPerPopper(numToPopPerPopper);

        CassQueueImpl cq = cqFactory.createInstance("test_" + System.currentTimeMillis(), 20000, 100, 4, 5000, false);
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

        assertEquals("waiting queue should be empty", 0, qRepos.getCountOfWaitingMsgs(cq.getName()).totalMsgCount);
        assertEquals("delivered queue should be empty", 0,
                qRepos.getCountOfPendingCommitMsgs(cq.getName()).totalMsgCount);
    }

    @Before
    public void setupTest() throws Exception {
        cqFactory = new CassQueueFactoryImpl(qRepos, new PipeDescriptorFactory(qRepos), new PipeLockerImpl());
    }

}
