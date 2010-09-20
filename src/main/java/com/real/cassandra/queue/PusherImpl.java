package com.real.cassandra.queue;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.pipes.PipeDescriptorFactory;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
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

    // injected objects
    private QueueRepositoryAbstractImpl qRepos;
    private boolean shutdownInProgress = false;
    private CassQueueImpl cq;
    private PipeDescriptorFactory pipeDescFactory;
    private CassQMsgFactory qMsgFactory = new CassQMsgFactory();

    private PipeDescriptorImpl pipeDesc = null;
    private long start;

    private int pushCount;

    private RollingStat pushStat;

    public PusherImpl(CassQueueImpl cq, QueueRepositoryAbstractImpl qRepos, PipeDescriptorFactory pipeDescFactory,
            RollingStat pushStat) {
        this.cq = (CassQueueImpl) cq;
        this.qRepos = qRepos;
        this.pipeDescFactory = pipeDescFactory;
        this.start = System.currentTimeMillis();
        this.pushStat = pushStat;
    }

    public CassQMsg push(String msgData) throws Exception {
        return insertInternal(qMsgFactory.createMsgId(), msgData);
    }

    public CassQMsg insert(CassQMsg qMsg) throws Exception {
        return insertInternal(qMsg.getMsgId(), qMsg.getMsgData());
    }

    private CassQMsg insertInternal(UUID msgId, String msgData) throws Exception {
        long start = System.currentTimeMillis();

        if (shutdownInProgress) {
            throw new IllegalStateException("cannot push messages when shutdown in progress");
        }

        if (isNewPipeNeeded()) {
            logger.debug("new pipe needed, creating one");
            createNewPipe();
        }

        pipeDesc.incMsgCount();
        pushCount++;

        CassQMsg qMsg = qMsgFactory.createInstance(pipeDesc, msgId, msgData);
        qRepos.insert(getQName(), pipeDesc, qMsg.getMsgId(), qMsg.getMsgData());
        logger.debug("pushed message : {}", qMsg);

        pushStat.addSample(System.currentTimeMillis() - start);
        return qMsg;

    }

    private void createNewPipe() throws Exception {
        PipeDescriptorImpl newPipeDesc =
                pipeDescFactory.createInstance(cq.getName(), PipeDescriptorImpl.STATUS_PUSH_ACTIVE, 0);

        // TODO:BTB optimize by combining setting each pipe's active status
        // set old pipeDesc as inactive
        if (null != pipeDesc) {
            qRepos.setPipeDescriptorStatus(pipeDesc, PipeDescriptorImpl.STATUS_PUSH_FINISHED);
        }

        pipeDesc = newPipeDesc;
        logger.debug("created new pipe : {}", pipeDesc);
        start = System.currentTimeMillis();
    }

    private boolean isNewPipeNeeded() {
        if (null == pipeDesc) {
            logger.debug("new pipe need, none exists");
            return true;
        }
        else if (pipeDesc.getMsgCount() >= cq.getMaxPushesPerPipe()) {
            logger.debug("new pipe need, msg count exceeds max of {}", cq.getMaxPushesPerPipe());
            return true;
        }
        else if (System.currentTimeMillis() - start > cq.getMaxPushTimePerPipe()) {
            logger.debug("new pipe need, pipe has exceed expiration of {} ms", cq.getMaxPushTimePerPipe());
            return true;
        }

        return false;
    }

    public String getQName() {
        return cq.getName();
    }

    public void shutdown() throws Exception {
        shutdownInProgress = true;
        qRepos.setPipeDescriptorStatus(pipeDesc, PipeDescriptorImpl.STATUS_PUSH_FINISHED);
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
