package com.real.cassandra.queue.pipeperpusher;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;

public class PopperImpl {
    private static Logger logger = LoggerFactory.getLogger(PopperImpl.class);

    private CassQueueImpl cq;
    private QueueRepositoryImpl qRepos;
    private boolean shutdownInProgress = false;
    private List<PipeDescriptorImpl> pipeDescList;
    private PipeLockerImpl popLocker;

    public PopperImpl(CassQueueImpl cq, QueueRepositoryImpl qRepos, PipeLockerImpl popLock) {
        this.cq = cq;
        this.qRepos = qRepos;
        this.popLocker = popLock;
    }

    public CassQMsg pop() {
        if (shutdownInProgress) {
            throw new IllegalStateException("cannot pop messages when shutdown in progress");
        }

        try {
            pipeDescList = qRepos.getOldestNonEmptyPipes(cq.getName(), cq.getPopWidth());
            PipeDescriptorImpl pipeDesc = null;
            try {
                pipeDesc = pickAndLockPipe();
                if (null == pipeDesc) {
                    return null;
                }
                return retrieveOldestMsgFromPipe(pipeDesc);
            }
            finally {
                popLocker.release(pipeDesc);
            }
        }
        catch (Exception e) {
            throw new CassQueueException("exception while pop'ing", e);
        }
    }

    private CassQMsg retrieveOldestMsgFromPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        CassQMsg qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc);
        if (null != qMsg) {
            qRepos.moveMsgFromWaitingToDeliveredPipe(pipeDesc, qMsg);
        }
        return qMsg;
    }

    private PipeDescriptorImpl pickAndLockPipe() {
        for (PipeDescriptorImpl pipeDesc : pipeDescList) {
            if (popLocker.lock(pipeDesc)) {
                logger.debug("locked pipe : " + pipeDesc);
                return pipeDesc;
            }
        }
        return null;
    }

    public String getQName() {
        return cq.getName();
    }

    public void shutdown() throws Exception {
        shutdownInProgress = true;

        // TODO:BTB do stuff here for shutdown
    }
}
