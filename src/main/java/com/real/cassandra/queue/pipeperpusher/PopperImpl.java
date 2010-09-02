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
    private PusherImpl rollbackPusher;

    public PopperImpl(CassQueueImpl cq, QueueRepositoryImpl qRepos, PipeLockerImpl popLock) throws Exception {
        this.cq = cq;
        this.qRepos = qRepos;
        this.popLocker = popLock;
        rollbackPusher = cq.createPusher();
        refreshPipeList();
    }

    public CassQMsg pop() {
        if (shutdownInProgress) {
            throw new IllegalStateException("cannot pop messages when shutdown in progress");
        }

        try {
            PipeDescriptorImpl pipeDesc = null;

            // make sure we try all pipes to get a msg
            for (int i = 0; i < cq.getPopWidth(); i++) {
                try {
                    pipeDesc = pickAndLockPipe();

                    // if can't find a pipe to lock, try again
                    if (null == pipeDesc) {
                        continue;
                    }

                    CassQMsg qMsg = retrieveOldestMsgFromPipe(pipeDesc);
                    if (null == qMsg) {
                        // if this pipe is finished then mark as empty since it
                        // has no more msgs
                        if (PipeDescriptorImpl.STATUS_PUSH_FINISHED.equals(pipeDesc.getStatus())) {
                            qRepos.setPipeDescriptorStatus(cq.getName(), pipeDesc,
                                    PipeDescriptorImpl.STATUS_FINISHED_AND_EMPTY);
                            refreshPipeList();
                            // finding a push finished empty pipe doesn't count
                            // against retries
                            i--;
                        }

                        // since null msg we try again until all pipes have been
                        // tried
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

    public void commit(CassQMsg qMsg) throws Exception {
        qRepos.removeMsgFromDeliveredPipe(qMsg);
    }

    public CassQMsg rollback(CassQMsg qMsg) throws Exception {
        CassQMsg qNewMsg = rollbackPusher.push(qMsg.getMsgData());
        qRepos.removeMsgFromDeliveredPipe(qMsg);
        return qNewMsg;
    }

    private void refreshPipeList() throws Exception {
        pipeDescList = qRepos.getOldestNonEmptyPipes(cq.getName(), cq.getPopWidth());
        nextPipeCounter = 0;
    }

    private CassQMsg retrieveOldestMsgFromPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        CassQMsg qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc);
        if (null != qMsg) {
            qRepos.moveMsgFromWaitingToDeliveredPipe(qMsg);
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

    public void forceRefresh() throws Exception {
        refreshPipeList();
    }
}
