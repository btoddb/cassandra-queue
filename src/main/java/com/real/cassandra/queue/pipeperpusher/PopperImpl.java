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
    private long nextPipeCounter;
    private int numRetries = 2;

    public PopperImpl(CassQueueImpl cq, QueueRepositoryImpl qRepos, PipeLockerImpl popLock) throws Exception {
        this.cq = cq;
        this.qRepos = qRepos;
        this.popLocker = popLock;
        refreshPipeList();
    }

    public CassQMsg pop() {
        if (shutdownInProgress) {
            throw new IllegalStateException("cannot pop messages when shutdown in progress");
        }

        try {
            PipeDescriptorImpl pipeDesc = null;

            for (int i = 0; i < numRetries; i++) {
                try {
                    pipeDesc = pickAndLockPipe();

                    // if can't find a pipe to lock, try again
                    if (null == pipeDesc) {
                        continue;
                    }

                    // locked pipe,
                    CassQMsg qMsg = retrieveOldestMsgFromPipe(pipeDesc);

                    // if no message returned, because pipe is empty and pushing
                    // has ended, then mark as empty and force retry
                    if (null == qMsg && PipeDescriptorImpl.STATUS_PUSH_FINISHED.equals(pipeDesc.getStatus())) {
                        qRepos.setPipeDescriptorStatus(cq.getName(), pipeDesc,
                                PipeDescriptorImpl.STATUS_FINISHED_AND_EMPTY);
                        refreshPipeList();
                        i--;
                        continue;
                    }
                    return qMsg;
                }
                finally {
                    if (null != pipeDesc) {
                        popLocker.release(pipeDesc);
                    }
                }
            }

            return null;
        }
        catch (Exception e) {
            throw new CassQueueException("exception while pop'ing", e);
        }
    }

    private void refreshPipeList() throws Exception {
        pipeDescList = qRepos.getOldestNonEmptyPipes(cq.getName(), cq.getPopWidth());
        nextPipeCounter = 0;
    }

    private CassQMsg retrieveOldestMsgFromPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        CassQMsg qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc);
        if (null != qMsg) {
            qRepos.moveMsgFromWaitingToDeliveredPipe(pipeDesc, qMsg);
        }
        return qMsg;
    }

    private PipeDescriptorImpl pickAndLockPipe() throws Exception {
        if (pipeDescList.isEmpty()) {
            refreshPipeList();
            if (pipeDescList.isEmpty()) {
                return null;
            }
        }

        int width = cq.getPopWidth();
        for (int i = 0; i < width; i++) {
            PipeDescriptorImpl pipeDesc = pipeDescList.get((int) ((nextPipeCounter++ + i) % width));
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
