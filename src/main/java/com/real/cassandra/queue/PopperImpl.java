package com.real.cassandra.queue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeLockerImpl;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
import com.real.cassandra.queue.utils.RollingStat;

public class PopperImpl {
    private static Logger logger = LoggerFactory.getLogger(PopperImpl.class);

    private Object pipeWatcherMonObj = new Object();
    private CassQueueImpl cq;
    private QueueRepositoryAbstractImpl qRepos;
    private boolean shutdownInProgress = false;
    private List<PipeDescriptorImpl> pipeDescList;
    private PipeLockerImpl popLocker;
    private long nextPipeCounter;
    private PusherImpl rollbackPusher;
    private PipeWatcher pipeWatcher;

    private RollingStat popNotEmptyStat;
    private RollingStat popEmptyStat;

    public PopperImpl(CassQueueImpl cq, QueueRepositoryAbstractImpl qRepos, PipeLockerImpl popLock,
            RollingStat popNotEmptyStat, RollingStat popEmptyStat) throws Exception {
        this.cq = cq;
        this.qRepos = qRepos;
        this.popLocker = popLock;
        this.popNotEmptyStat = popNotEmptyStat;
        this.popEmptyStat = popEmptyStat;
    }

    public void initialize(boolean startPipeWatcher) {
        rollbackPusher = cq.createPusher();
        if (startPipeWatcher) {
            pipeWatcher = new PipeWatcher();
            pipeWatcher.start();
        }
    }

    public CassQMsg pop() {
        long start = System.currentTimeMillis();

        if (shutdownInProgress) {
            throw new IllegalStateException("cannot pop messages when shutdown in progress");
        }

        try {
            PipeDescriptorImpl pipeDesc = null;

            // make sure we try all pipes to get a msg
            for (int i = 0; i < cq.getMaxPopWidth(); i++) {
                try {
                    pipeDesc = pickAndLockPipe();

                    // if can't find a pipe to lock, try again
                    if (null == pipeDesc) {
                        continue;
                    }

                    CassQMsg qMsg;
                    try {
                        qMsg = retrieveOldestMsgFromPipe(pipeDesc);
                    }
                    catch (Exception e) {
                        logger.error("exception while getting oldest msg from waiting pipe : " + pipeDesc.getPipeId(),
                                e);
                        forceRefresh();
                        continue;
                    }

                    if (null != qMsg) {
                        popNotEmptyStat.addSample(System.currentTimeMillis() - start);
                        return qMsg;
                    }

                    // pipe didn't have unpopped msg so mark it as finished if
                    // needed then try again
                    markPipeAsFinishedIfNeeded(pipeDesc);
                }
                finally {
                    if (null != pipeDesc) {
                        popLocker.release(pipeDesc);
                    }
                }
            }

            popEmptyStat.addSample(System.currentTimeMillis() - start);
            return null;
        }
        catch (Exception e) {
            throw new CassQueueException("exception while pop'ing", e);
        }
    }

    private void markPipeAsFinishedIfNeeded(PipeDescriptorImpl pipeDesc) throws Exception {
        // if this pipe is finished then mark as empty since it
        // has no more msgs
        if (PipeDescriptorImpl.STATUS_PUSH_FINISHED.equals(pipeDesc.getStatus())) {
            qRepos.setPipeDescriptorStatus(pipeDesc, PipeDescriptorImpl.STATUS_FINISHED_AND_EMPTY);
            forceRefresh();
        }
    }

    public void commit(CassQMsg qMsg) throws Exception {
        qRepos.removeMsgFromPendingPipe(qMsg);
    }

    public CassQMsg rollback(CassQMsg qMsg) throws Exception {
        CassQMsg qNewMsg = rollbackPusher.push(qMsg.getMsgData());
        qRepos.removeMsgFromPendingPipe(qMsg);
        return qNewMsg;
    }

    private void refreshPipeList() throws Exception {
        synchronized (pipeWatcherMonObj) {
            pipeDescList = qRepos.getOldestNonEmptyPipes(cq.getName(), cq.getMaxPopWidth());
            // do not reset counter as this can cause pipes to never be queried
            // if pushes have slowed (or stopped) and a large pop width
            // nextPipeCounter = 0;
            if (logger.isDebugEnabled()) {
                for (PipeDescriptorImpl pipeDesc : pipeDescList) {
                    logger.debug(pipeDesc.toString());
                }
            }
        }
    }

    private CassQMsg retrieveOldestMsgFromPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        CassQMsg qMsg = qRepos.getOldestMsgFromWaitingPipe(pipeDesc);
        if (null != qMsg) {
            qRepos.moveMsgFromWaitingToPendingPipe(qMsg);
        }
        return qMsg;
    }

    private PipeDescriptorImpl pickAndLockPipe() throws Exception {
        if (null == pipeDescList || pipeDescList.isEmpty()) {
            forceRefresh();
            logger.debug("no non-empty non-finished pipe descriptors found - signaled refresh");
            return null;
        }

        // sync with the pipe watcher thread
        synchronized (pipeWatcherMonObj) {
            int width = pipeDescList.size() < cq.getMaxPopWidth() ? pipeDescList.size() : cq.getMaxPopWidth();
            for (int i = 0; i < width; i++) {
                PipeDescriptorImpl pipeDesc = pipeDescList.get((int) (nextPipeCounter++ % width));
                if (popLocker.lock(pipeDesc)) {
                    logger.debug("locked pipe : " + pipeDesc);
                    return pipeDesc;
                }
            }
        }
        return null;
    }

    public String getQName() {
        return cq.getName();
    }

    public void shutdown() throws Exception {
        shutdownInProgress = true;
        if (null != pipeWatcher) {
            pipeWatcher.stopProcessing();
        }

        // TODO:BTB do stuff here for shutdown
    }

    public void forceRefresh() throws Exception {
        if (null != pipeWatcher) {
            pipeWatcher.refreshNow();
        }
        else {
            refreshPipeList();
        }
    }

    /**
     * Watches the list of pipe descriptors updating as periodically.
     * 
     * @author Todd Burruss
     */
    class PipeWatcher implements Runnable {
        private boolean stopProcessing;
        private Thread theThread;

        public void start() {
            theThread = new Thread(this);
            theThread.setDaemon(true);
            theThread.setName(getClass().getSimpleName());
            theThread.start();

            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                // ignore
                Thread.interrupted();
            }
        }

        public void refreshNow() {
            theThread.interrupt();
        }

        public void stopProcessing() {
            stopProcessing = true;
        }

        @Override
        public void run() {
            while (!stopProcessing) {
                try {
                    refreshPipeList();
                }
                catch (Exception e) {
                    logger.error("exception while refreshing pipe list", e);
                }

                try {
                    Thread.sleep(cq.getPopPipeRefreshDelay());
                }
                catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        }
    }

    public long getPopCalledCount() {
        return getPopNotEmptyCount() + getPopEmptyCount();
    }

    public long getPopNotEmptyCount() {
        return popNotEmptyStat.getTotalSamplesProcessed();
    }

    public long getPopEmptyCount() {
        return popEmptyStat.getTotalSamplesProcessed();
    }
}
