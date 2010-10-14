package com.real.cassandra.queue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.locks.LocalLockerImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;

public class PipeReaper implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(PipeReaper.class);

    private Thread theThread;
    private boolean stopProcessing = false;
    private LocalLockerImpl queueStatsLocker;
    private int queueStatsLockRetryLimit = 5;
    private long queueStatsLockRetryDelay = 10;
    private String qName;
    private QueueRepositoryImpl qRepos;
    private long processingDelay = 10000;

    public PipeReaper(String qName, QueueRepositoryImpl qRepos, LocalLockerImpl queueStatsLocker) {
        this.qName = qName;
        this.qRepos = qRepos;
        this.queueStatsLocker = queueStatsLocker;
    }

    public void start() {
        theThread = new Thread(this);
        theThread.setName(this.getClass().getSimpleName());
        theThread.start();
    }

    public void run() {
        try {
            while (!stopProcessing) {
                if (queueStatsLocker.lock(qName, queueStatsLockRetryLimit, queueStatsLockRetryDelay)) {
                    try {
                        rollUpStatsFromPushFinishedPipes();
                        rollUpStatsFromPopFinishedPipes();
                        removeCompletedPipes();
                    }
                    catch (Throwable e) {
                        logger.error("exception while reaping finished and empty pipes", e);
                    }
                    finally {
                        queueStatsLocker.release(qName);
                    }
                }
                else {
                    logger.debug("could not lock 'queue stats' for updating queue, {}", qName);
                }

                try {
                    Thread.sleep(processingDelay);
                }
                catch (InterruptedException e) {
                    Thread.interrupted();
                    // nothing else to do
                }
            }
        }
        catch (Throwable e) {
            logger.error("exception while update stats and reaping pipes", e);
        }
    }

    public void wakeUp() {
        theThread.interrupt();
    }

    private void rollUpStatsFromPushFinishedPipes() {
        // pipes in this status are no longer used by pusher client, so no need
        // for locking
        List<PipeDescriptorImpl> pipeList = qRepos.getPushNotActivePipes(qName, 10);
        for (PipeDescriptorImpl pipeDesc : pipeList) {
            rollUpPushStatsFromPipe(pipeDesc);
        }
    }

    private void rollUpStatsFromPopFinishedPipes() {
        // pipes in this status are no longer used by popper client, so no need
        // for locking
        List<PipeDescriptorImpl> pipeList = qRepos.getPopFinishedPipes(qName, 10);
        for (PipeDescriptorImpl pipeDesc : pipeList) {
            rollUpPopStatsFromPipe(pipeDesc);
        }
    }

    private void removeCompletedPipes() {
        // pipes in this status have there stats aggregated and are no longer
        // used, therefore we can be remove them
        List<PipeDescriptorImpl> pipeList = qRepos.getCompletedPipes(qName, 10);
        for (PipeDescriptorImpl pipeDesc : pipeList) {
            qRepos.removePipeDescriptor(pipeDesc);
        }
    }

    private void rollUpPushStatsFromPipe(PipeDescriptorImpl pipeDesc) {
        QueueStats qStats = qRepos.getQueueStats(qName);
        qStats.incTotalPushes(pipeDesc.getPushCount());
        // TODO:BTB may want to break this into its on 'insert' call
        qRepos.updateQueueStats(qStats);
        qRepos.updatePipePushStatus(pipeDesc, PipeStatus.COMPLETED);
    }

    private void rollUpPopStatsFromPipe(PipeDescriptorImpl pipeDesc) {
        QueueStats qStats = qRepos.getQueueStats(qName);
        // TODO:BTB this should be changed to getPopCount when implemented
        qStats.incTotalPops(pipeDesc.getPushCount());
        // TODO:BTB may want to break this into its on 'insert' call
        qRepos.updateQueueStats(qStats);
        qRepos.updatePipePopStatus(pipeDesc, PipeStatus.COMPLETED);
    }

    // private void updatePipeStatsWithRetry(PipeDescriptorImpl pipeDesc,
    // QueueStats qStats, PipeStatus pushStatus,
    // PipeStatus popStatus) {
    // int numRetries = 5;
    // for (int i = 0; i < numRetries; i++) {
    // try {
    // // the stats are frozen because of locking so can be retried
    // // without worry about adding too many
    // qRepos.updateQueueStats(qStats);
    // break;
    // }
    // catch (Throwable e) {
    // logger.error("exception while updating queue stats and setting push/pop status",
    // e);
    // try {
    // Thread.sleep(50);
    // }
    // catch (InterruptedException ee) {
    // Thread.interrupted();
    // // do nothing else
    // }
    // }
    // }
    // }

    public void shutdown() {
        stopProcessing = true;
        theThread.interrupt();
    }

    public void setProcessingDelay(long processingDelay) {
        this.processingDelay = processingDelay;
    }
}
