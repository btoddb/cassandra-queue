package com.real.cassandra.queue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.locks.LocalLockerImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.utils.RollingStat;

public class PopperImpl {
    private static Logger logger = LoggerFactory.getLogger(PopperImpl.class);

    private static final int PUSHER_TIMEOUT_DELAY = 10000;
    private Object pipeWatcherMonObj = new Object();
    private CassQueueImpl cq;
    private QueueRepositoryImpl qRepos;
    private boolean shutdownInProgress = false;
    private List<PipeDescriptorImpl> pipeDescList;
    private LocalLockerImpl popLocker;
    private long nextPipeCounter;
    private PipeWatcher pipeWatcher;

    private RollingStat popNotEmptyStat;
    private RollingStat popEmptyStat;

    public PopperImpl(CassQueueImpl cq, QueueRepositoryImpl qRepos, LocalLockerImpl popLocker,
            RollingStat popNotEmptyStat, RollingStat popEmptyStat) {
        this.cq = cq;
        this.qRepos = qRepos;
        this.popLocker = popLocker;
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
                        qRepos.updatePipePopCount(pipeDesc, pipeDesc.incPopCount());
                        return qMsg;
                    }

                    checkIfPipeCanBeMarkedPopFinished(pipeDesc);
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

    /**
     * If pipe is {@link PipeStatus#PUSH_FINISHED} or expired then mark as
     * {@link PipeStatus#NOT_ACTIVE}. This method marks expired pipes as
     * {@link PipeStatus#PUSH_FINISHED} instead of {@link PusherImpl} to avoid
     * race condition.
     * 
     * @param pipeDesc
     */
    private void checkIfPipeCanBeMarkedPopFinished(PipeDescriptorImpl pipeDesc) {
        // if this pipe is finished or expired, mark as pop finished
        if (!pipeDesc.isPushActive()) {
            // no race condition here with push status because the pusher is no
            // longer active
            markPipeAsPopNotActiveAndRefresh(pipeDesc);
            logger.debug("pipe is not push active and empty, marking pop not active: {}", pipeDesc.toString());
        }
        else if (pipeTimeoutExpired(pipeDesc)) {
            // no race condition here because the pusher does not do anything
            // with expired pipes
            qRepos.updatePipePushStatus(pipeDesc, PipeStatus.NOT_ACTIVE);

            markPipeAsPopNotActiveAndRefresh(pipeDesc);
            logger.debug("pipe has expired, marking pop not active : {}", pipeDesc.toString());
        }
        else {
            logger.debug("pipe is not ready to be removed : {}", pipeDesc);
        }
    }

    private void markPipeAsPopNotActiveAndRefresh(PipeDescriptorImpl pipeDesc) {
        qRepos.updatePipePopStatus(pipeDesc, PipeStatus.NOT_ACTIVE);
        // TODO BTB:don't think we need to force wakeup just to count the stats.
        // the reaper has its own delay period
        // pipeReaper.wakeUp();

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

    public void commit(CassQMsg qMsg) {
        cq.commit(qMsg);
    }

    public CassQMsg rollback(CassQMsg qMsg) throws Exception {
        return cq.rollback(qMsg);
    }

    private void refreshPipeList() {
        synchronized (pipeWatcherMonObj) {
            pipeDescList = qRepos.getOldestPopActivePipes(cq.getName(), cq.getMaxPopWidth());
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

    public void shutdown() {
        shutdownInProgress = true;
        if (null != pipeWatcher) {
            pipeWatcher.stopProcessing();
        }

        // TODO:BTB do stuff here for shutdown
    }

    public void forceRefresh() {
        if (null != pipeWatcher) {
            pipeWatcher.wakeUp();
        }
        else {
            refreshPipeList();
        }
    }

    /**
     * Watches the list of pipe descriptors updating periodically.
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
                // TODO BTB:do we need this?
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                // ignore
                Thread.interrupted();
            }
        }

        public void wakeUp() {
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
