package com.real.cassandra.queue.pipeperpusher;

import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;

import com.real.cassandra.queue.CassQMsg;

/**
 * An instance of {@link Pusher} that implements a "pipe per pusher thread"
 * model using Cassandra as persistent storage. This object is not thread safe!
 * As the previous statement implies one {@link PusherImpl} instance is required
 * per thread. object is not guarateed thread safe.
 * 
 * @author Todd Burruss
 */
public class PusherImpl {
    private static MyInetAddress inetAddr = new MyInetAddress();

    // injected objects
    private QueueRepositoryImpl qRepos;
    private boolean shutdownInProgress = false;
    private CassQueueImpl cq;
    private PipeDescriptorFactory pipeDescFactory;
    private CassQMsgFactory qMsgFactory = new CassQMsgFactory();

    private PipeDescriptorImpl pipeDesc = null;
    private long start;

    public PusherImpl(CassQueueImpl cq, QueueRepositoryImpl qRepos, PipeDescriptorFactory pipeDescFactory) {
        this.cq = (CassQueueImpl) cq;
        this.qRepos = qRepos;
        this.pipeDescFactory = pipeDescFactory;
        this.start = System.currentTimeMillis();
    }

    public CassQMsg push(String msgData) throws Exception {
        if (shutdownInProgress) {
            throw new IllegalStateException("cannot push messages when shutdown in progress");
        }

        if (isNewPipeNeeded()) {
            createNewPipe();
        }

        pipeDesc.incMsgCount();

        CassQMsg qMsg = qMsgFactory.createInstance(pipeDesc, msgData);
        qRepos.insert(getQName(), pipeDesc, qMsg.getMsgId(), msgData);
        return qMsg;
    }

    private void createNewPipe() throws Exception {
        UUID pipeId = UUIDGen.makeType1UUIDFromHost(inetAddr.get());
        qRepos.createPipeDescriptor(cq.getName(), pipeId, PipeDescriptorImpl.STATUS_PUSH_ACTIVE);

        // TODO:BTB optimize by combining setting each pipe's active status
        // set old pipeDesc as inactive
        if (null != pipeDesc) {
            qRepos.setPipeDescriptorStatus(cq.getName(), pipeDesc, PipeDescriptorImpl.STATUS_PUSH_FINISHED);
        }

        pipeDesc = pipeDescFactory.createInstance(cq.getName(), pipeId, PipeDescriptorImpl.STATUS_PUSH_ACTIVE, 0);
        start = System.currentTimeMillis();
    }

    private boolean isNewPipeNeeded() {
        return null == pipeDesc || pipeDesc.getMsgCount() >= cq.getMaxPushesPerPipe()
                || System.currentTimeMillis() - start > cq.getMaxPushTimeOfPipe();
    }

    public String getQName() {
        return cq.getName();
    }

    public void shutdown() throws Exception {
        shutdownInProgress = true;
        qRepos.setPipeDescriptorStatus(cq.getName(), pipeDesc, PipeDescriptorImpl.STATUS_PUSH_FINISHED);
    }

    public long getMaxPushTimeOfPipe() {
        return cq.getMaxPushTimeOfPipe();
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

}
