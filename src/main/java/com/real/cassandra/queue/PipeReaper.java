package com.real.cassandra.queue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.locks.Locker;
import com.real.cassandra.queue.locks.ObjectLock;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;

public class PipeReaper implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(PipeReaper.class);

    private Thread theThread;
    private boolean stopProcessing = false;
    private Locker<QueueDescriptor> queueStatsLocker;
    private int queueStatsLockRetryLimit = 5;
    private long queueStatsLockRetryDelay = 10;
    private QueueDescriptor qDesc;
    private QueueRepositoryImpl qRepos;
    private long processingDelay = 10000;

    public PipeReaper(QueueDescriptor qDesc, QueueRepositoryImpl qRepos, Locker<QueueDescriptor> queueStatsLocker) {
        this.qDesc = qDesc;
        this.qRepos = qRepos;
        this.queueStatsLocker = queueStatsLocker;
    }

    public void start() {
        theThread = new Thread(this);
        theThread.setName(this.getClass().getSimpleName());
        theThread.start();
    }

    public void run() {
        while (!stopProcessing) {
            try {
                ObjectLock<QueueDescriptor> lock =
                        queueStatsLocker.lock(qDesc, queueStatsLockRetryLimit, queueStatsLockRetryDelay);
                if (lock != null) {
                    try {
                        rollUpStatsFromPushFinishedPipes();
                        rollUpStatsFromPopFinishedPipes();
                        removeCompletedPipes();
                    }
                    finally {
                        queueStatsLocker.release(lock);
                    }
                }
                else {
                    logger.debug("could not lock 'queue stats' for updating queue, {}", qDesc.getName());
                }
            }
            catch (Throwable e) {
                logger.error("exception while reaping pipes", e);
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

    public void wakeUp() {
        theThread.interrupt();
    }

    private void rollUpStatsFromPushFinishedPipes() {
        // pipes in this status are no longer used by pusher client, so no need
        // for locking
        List<PipeDescriptorImpl> pipeList = qRepos.getPushNotActivePipes(qDesc.getName(), 10);
        for (PipeDescriptorImpl pipeDesc : pipeList) {
            logger.debug("Rolling up stats for push \"not active\" pipe, {}", pipeDesc.getPipeId());
            rollUpPushStatsFromPipe(pipeDesc);
        }
    }

    private void rollUpStatsFromPopFinishedPipes() {
        // pipes in this status are no longer used by popper client, so no need
        // for locking
        List<PipeDescriptorImpl> pipeList = qRepos.getPopFinishedPipes(qDesc.getName(), 10);
        for (PipeDescriptorImpl pipeDesc : pipeList) {
            logger.debug("Rolling up status for pop \"not active\" pipe, {}", pipeDesc.getPipeId());
            rollUpPopStatsFromPipe(pipeDesc);
        }
    }

    private void removeCompletedPipes() {
        // pipes in this status have there stats aggregated and are no longer
        // used, therefore we can be remove them
        List<PipeDescriptorImpl> pipeList = qRepos.getCompletedPipes(qDesc.getName(), 10);
        for (PipeDescriptorImpl pipeDesc : pipeList) {
            logger.debug("Removing completed pipe, {}", pipeDesc.getPipeId());
            qRepos.removePipeDescriptor(pipeDesc);
        }
    }

    private void rollUpPushStatsFromPipe(PipeDescriptorImpl pipeDesc) {
        QueueStats qStats = qRepos.getQueueStats(qDesc.getName());
        qStats.incTotalPushes(pipeDesc.getPushCount());
        // TODO:BTB may want to break this into its on 'insert' call
        qRepos.updateQueueStats(qStats);
        qRepos.updatePipePushStatus(pipeDesc, PipeStatus.COMPLETED);
    }

    private void rollUpPopStatsFromPipe(PipeDescriptorImpl pipeDesc) {
        QueueStats qStats = qRepos.getQueueStats(qDesc.getName());
        // TODO:BTB this should be changed to getPopCount when implemented
        qStats.incTotalPops(pipeDesc.getPushCount());
        // TODO:BTB may want to break this into its on 'insert' call
        qRepos.updateQueueStats(qStats);
        qRepos.updatePipePopStatus(pipeDesc, PipeStatus.COMPLETED);
    }

    public void shutdownAndWait() {
        stopProcessing = true;
        theThread.interrupt();
        while (theThread.isAlive()) {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                // do nothing
            }
        }
    }

    public void setProcessingDelay(long processingDelay) {
        this.processingDelay = processingDelay;
    }
}