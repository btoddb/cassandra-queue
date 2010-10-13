package com.real.cassandra.queue;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.pipes.PipeDescriptorFactory;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.utils.RollingStat;

/**
 * An instance of {@link Pusher} that implements a "pipe per pusher thread"
 * model using Cassandra as persistent storage. This object is not thread safe!
 * As the previous statement implies one {@link PusherImpl} instance is required
 * per thread. object is not guarateed thread safe.
 * 
 * @author Todd Burruss
 */
public class PusherImpl {
    private static Logger logger = LoggerFactory.getLogger(PusherImpl.class);

    private QueueRepositoryImpl qRepos;
    private boolean shutdownInProgress = false;
    private CassQueueImpl cq;
    private PipeDescriptorFactory pipeDescFactory;
    private CassQMsgFactory qMsgFactory = new CassQMsgFactory();
    private PipeReaper pipeReaper;
    private PipeDescriptorImpl pipeDesc = null;

    private int pushCount;

    private RollingStat pushStat;

    public PusherImpl(CassQueueImpl cq, QueueRepositoryImpl qRepos, PipeDescriptorFactory pipeDescFactory,
            PipeReaper pipeReaper, RollingStat pushStat) {
        this.cq = (CassQueueImpl) cq;
        this.qRepos = qRepos;
        this.pipeDescFactory = pipeDescFactory;
        this.pipeReaper = pipeReaper;
        this.pushStat = pushStat;
    }

    public CassQMsg push(String msgData) {
        return insertInternal(qMsgFactory.createMsgId(), msgData);
    }

    private CassQMsg insertInternal(UUID msgId, String msgData) {
        long start = System.currentTimeMillis();

        if (shutdownInProgress) {
            throw new IllegalStateException("cannot push messages when shutdown in progress");
        }

        if (markPipeFinishedIfNeeded()) {
            logger.debug("new pipe needed, switching to new one");
            switchToNewPipe();
        }

        pipeDesc.incMsgCount();
        pushCount++;

        CassQMsg qMsg = qMsgFactory.createInstance(pipeDesc, msgId, msgData);
        qRepos.insertMsg(pipeDesc, qMsg.getMsgId(), qMsg.getMsgData());
        logger.debug("pushed message : {}", qMsg);

        pushStat.addSample(System.currentTimeMillis() - start);
        return qMsg;
    }

    private void switchToNewPipe() {
        PipeDescriptorImpl newPipeDesc = createNewPipe();
        pipeDesc = newPipeDesc;
        logger.debug("switched to new pipe : {}", pipeDesc);
    }

    private PipeDescriptorImpl createNewPipe() {
        return pipeDescFactory.createInstance(cq.getName(), PipeStatus.ACTIVE, PipeStatus.ACTIVE, 0);
    }

    /**
     * If pipe is full, but not expired then mark it as
     * {@link PipeStatus#PUSH_FINISHED}. If it has expired, {@link PopperImpl}
     * will handle this case to prevent race condition.
     * 
     * @return
     */
    private boolean markPipeFinishedIfNeeded() {
        if (null == pipeDesc) {
            logger.debug("new pipe needed, none exists");
            return true;
        }
        else if (System.currentTimeMillis() - pipeDesc.getStartTimestamp() > cq.getMaxPushTimePerPipe()) {
            logger.debug("new pipe needed, pipe has exceed expiration of {} ms", cq.getMaxPushTimePerPipe());
            return true;
        }
        else if (pipeDesc.getMsgCount() >= cq.getMaxPushesPerPipe()) {
            logger.debug("new pipe needed, msg count exceeds max of {}", cq.getMaxPushesPerPipe());
            qRepos.updatePipeStatus(pipeDesc, PipeStatus.NOT_ACTIVE, pipeDesc.getPopStatus());
            pipeReaper.wakeUp();
            return true;
        }
        else {
            return false;
        }
    }

    public String getQName() {
        return cq.getName();
    }

    public void shutdown() {
        shutdownInProgress = true;
        qRepos.updatePipeStatus(pipeDesc, PipeStatus.NOT_ACTIVE, pipeDesc.getPopStatus());
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
        return pushCount;
    }

}
