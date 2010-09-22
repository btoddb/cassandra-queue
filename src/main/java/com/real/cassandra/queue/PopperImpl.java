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

    private static final int PUSHER_TIMEOUT_DELAY = 10000;
    private Object pipeWatcherMonObj = new Object();
    private CassQueueImpl cq;
    private QueueRepositoryAbstractImpl qRepos;
    private boolean shutdownInProgress = false;
    private List<PipeDescriptorImpl> pipeDescList;
    private PipeLockerImpl popLocker;
    private long nextPipeCounter;
    private PipeWatcher pipeWatcher;

    private RollingStat popNotEmptyStat;
    private RollingStat popEmptyStat;

    public PopperImpl(CassQueueImpl cq, QueueRepositoryAbstractImpl qRepos, PipeLockerImpl popLock,
            RollingStat popNotEmptyStat, RollingStat popEmptyStat) {
        this.cq = cq;
        this.qRepos = qRepos;
        this.popLocker = popLock;
        this.popNotEmptyStat = popNotEmptyStat;
        this.popEmptyStat = popEmptyStat;
    }

    public void initialize(boolean startPipeWatcher) {
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

            // make sure we try all pipes to get a msg. we go +1 for the case
            // where only maxPopPipeWidth = 1 and haven't loaded any pipe
            // descriptors yet
            for (int i = 0; i < cq.getMaxPopWidth() + 1; i++) {
                logger.debug("iteration {} of {}", i, cq.getMaxPopWidth() + 1);

                try {
                    pipeDesc = pickAndLockPipe();

                    // if can't find a pipe to lock, try again
                    if (null == pipeDesc) {
                        logger.debug("could not find a pipe to lock, trying again if retries left");
                        continue;
                    }

                    CassQMsg qMsg;
                    try {
                        qMsg = retrieveOldestMsgFromPipe(pipeDesc);
                    }
                    catch (Exception e) {
                        logger.error("exception while getting oldest msg from waiting pipe : {}", pipeDesc.getPipeId(),
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
                    checkIfPipeCanBeRemoved(pipeDesc);
                }
                finally {
                    if (null != pipeDesc) {
                        logger.debug("releasing pipe : {}", pipeDesc.toString());
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

    private void checkIfPipeCanBeRemoved(PipeDescriptorImpl pipeDesc) throws Exception {
        // if this pipe is finished or expired, mark as empty
        if (PipeDescriptorImpl.STATUS_PUSH_FINISHED.equals(pipeDesc.getStatus())) {
            markPipeAsFinishedAndRefresh(pipeDesc);
            logger.debug("pipe is push finished and empty, marked finished and removed pipe from descriptor list : {}",
                    pipeDesc.toString());
        }
        else if (pipeTimeoutExpired(pipeDesc)) {
            markPipeAsFinishedAndRefresh(pipeDesc);
            logger.debug("pipe has expired, marked finished and removed pipe from descriptor list : {}",
                    pipeDesc.toString());
        }
        else {
            logger.debug("pipe is not ready to be removed : {}", pipeDesc);
        }
    }

    private void markPipeAsFinishedAndRefresh(PipeDescriptorImpl pipeDesc) throws Exception {
        qRepos.setPipeDescriptorStatus(pipeDesc, PipeDescriptorImpl.STATUS_FINISHED_AND_EMPTY);

        // assuming only one pipe was removed during this refresh we can
        // perform better by reducing the counter so a "retry" occurs
        nextPipeCounter--;
        forceRefresh();
    }

    private boolean pipeTimeoutExpired(PipeDescriptorImpl pipeDesc) {
        // give the extra PUSHER_TIMEOUT_DELAY so no contention with pusher
        return System.currentTimeMillis() - pipeDesc.getStartTimestamp() - PUSHER_TIMEOUT_DELAY > cq
                .getMaxPushTimePerPipe();
    }

    public void commit(CassQMsg qMsg) throws Exception {
        cq.commit(qMsg);
    }

    public CassQMsg rollback(CassQMsg qMsg) throws Exception {
        return cq.rollback(qMsg);
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
            logger.debug("found message, moving it to 'waiting' pipe : {}", qMsg.toString());
            qRepos.moveMsgFromWaitingToPendingPipe(qMsg);
        }
        return qMsg;
    }

    private PipeDescriptorImpl pickAndLockPipe() throws Exception {
        if (null == pipeDescList || pipeDescList.isEmpty()) {
            logger.debug("no non-empty non-finished pipe descriptors found - signaled refresh");
            forceRefresh();
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
            logger.debug("no pipe available for locking, num pipes tried = {}", width);
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
