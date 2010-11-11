package com.real.cassandra.queue.app;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQueueImpl;

public abstract class WorkerThreadWatcher implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(WorkerThreadWatcher.class);

    protected QueueProperties envProps;
    protected List<PushPopAbstractBase> workerList;
    protected CassQueueImpl cq;

    private boolean stopProcessing = false;

    private Thread theThread;

    public WorkerThreadWatcher(QueueProperties envProps, CassQueueImpl cq) {
        this.envProps = envProps;
        this.cq = cq;
        this.workerList = new ArrayList<PushPopAbstractBase>();
    }

    public void start() {
        theThread = new Thread(this);
        theThread.setDaemon(true);
        theThread.setName(getClass().getSimpleName());
        theThread.start();
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

    @Override
    public void run() {
        while (!stopProcessing) {
            if (getTargetSize() != workerList.size()) {
                try {
                    adjustWorkers();
                }
                catch (Exception e) {
                    logger.error("exception while adjusting workers", e);
                }
            }
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                logger.debug("interrupted while sleeping - wakeup!");
                Thread.interrupted();
            }
        }

        shutdownAllWorkers();
    }

    private void shutdownAllWorkers() {
        for (PushPopAbstractBase worker : workerList) {
            worker.shutdown();
        }

        boolean stillWorking = false;
        do {
            for (PushPopAbstractBase worker : workerList) {
                stillWorking = stillWorking || worker.isThreadAlive();
                break;
            }

            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                // ignore
            }
        } while (stillWorking);
    }

    private void adjustWorkers() throws Exception {
        while (getTargetSize() != workerList.size()) {
            int newSize = getTargetSize();
            int currSize = workerList.size();
            if (newSize < currSize) {
                PushPopAbstractBase popper = workerList.remove(newSize);
                popper.shutdown();
            }
            else if (newSize > currSize) {
                addWorker();
            }
        }
    }

    protected abstract int getTargetSize();

    protected abstract void addWorker() throws Exception;

    public List<PushPopAbstractBase> getWorkerList() {
        return workerList;
    }
}
