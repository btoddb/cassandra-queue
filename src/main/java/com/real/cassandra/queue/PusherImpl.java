package com.real.cassandra.queue;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.utils.MyIp;
import com.real.cassandra.queue.utils.RollingStat;

/**
 * An instance of {@link PusherImpl} that implements a "pipe per pusher thread"
 * model using Cassandra as persistent storage. This object is thread safe.
 * 
 * @author Todd Burruss
 */
public class PusherImpl {
    private static Logger logger = LoggerFactory.getLogger(PusherImpl.class);

    private QueueRepositoryImpl qRepos;
    private boolean shutdownInProgress = false;
    private CassQueueImpl cq;
    private CassQMsgFactory qMsgFactory = new CassQMsgFactory();
    private PipeDescriptorImpl pipeDesc = null;
    private boolean working = false;

    private AtomicInteger pushCount = new AtomicInteger(0);

    private RollingStat pushStat;

    private Object pipeSwitcherMonitor = new Object();

    public PusherImpl(CassQueueImpl cq, QueueRepositoryImpl qRepos, RollingStat pushStat) {
        this.cq = cq;
        this.qRepos = qRepos;
        this.pushStat = pushStat;
    }

    public CassQMsg push(String msgData) {
        return push(msgData.getBytes());
    }

    public CassQMsg push(byte[] msgData) {
        // for shutdown sync'ing
        working = true;
        try {
            return insertInternal(qMsgFactory.createMsgId(), msgData);
        }
        finally {
            working = false;
        }
    }

    private CassQMsg insertInternal(UUID msgId, byte[] msgData) {
        long start = System.currentTimeMillis();

        if (shutdownInProgress) {
            throw new IllegalStateException("cannot push messages when shutdown in progress");
        }

        // pusher can be used by multiple threads
        synchronized (pipeSwitcherMonitor) {
            if (markPipeFinishedIfNeeded()) {
                logger.debug("new pipe needed, switching to new one");
                switchToNewPipe();
            }
        }

        pipeDesc.incPushCount();
        pushCount.incrementAndGet();

        CassQMsg qMsg = qRepos.insertMsg(pipeDesc, msgId, msgData);
        logger.debug("pushed message : {}", qMsg);

        pushStat.addSample(System.currentTimeMillis() - start);
        return qMsg;
    }

    private void switchToNewPipe() {
        pipeDesc = createNewPipe();
        logger.debug("switched to new pipe : {}", pipeDesc);
    }

    private PipeDescriptorImpl createNewPipe() {
        return qRepos.createPipeDescriptor(cq.getName(), UUIDGen.makeType1UUIDFromHost(MyIp.get()));
    }

    /**
     * If pipe is full, but not expired then mark it as
     * {@link PipeStatus#NOT_ACTIVE}. If it has expired, {@link PopperImpl} will
     * handle this case to prevent race condition.
     * 
     * @return true if new pipe needed
     */
    private boolean markPipeFinishedIfNeeded() {
        if (null == pipeDesc) {
            logger.debug("new pipe needed, none exists");
            return true;
        }
        else if (System.currentTimeMillis() - pipeDesc.getPushStartTimestamp() > cq.getMaxPushTimePerPipe()) {
            logger.debug("new pipe needed, pipe has exceed expiration of {} ms", cq.getMaxPushTimePerPipe());
            return true;
        }
        else if (pipeDesc.getPushCount() >= cq.getMaxPushesPerPipe()) {
            logger.debug("new pipe needed, msg count exceeds max of {}", cq.getMaxPushesPerPipe());

            // TODO BTB:could combine this with 'insert' to cut down on wire
            // time if needed
            qRepos.updatePipePushStatus(pipeDesc, PipeStatus.NOT_ACTIVE);

            return true;
        }
        else {
            return false;
        }
    }

    public String getQName() {
        return cq.getName();
    }

    public void shutdownAndWait() {
        shutdownInProgress = true;
        while (working) {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                // do nothing
            }
        }

        if (null != pipeDesc) {
            synchronized (pipeSwitcherMonitor) {
                qRepos.updatePipePushStatus(pipeDesc, PipeStatus.NOT_ACTIVE);
                pipeDesc = null;
            }
        }
    }

    public long getMaxPushTimeOfPipe() {
        return cq.getMaxPushTimePerPipe();
    }

    public int getMaxPushesPerPipe() {
        return cq.getMaxPushesPerPipe();
    }

    public boolean isShutdownInProgress() {
        return shutdownInProgress;
    }

    public PipeDescriptorImpl getPipeDesc() {
        return pipeDesc;
    }

    public int getPushCount() {
        return pushCount.get();
    }

}
