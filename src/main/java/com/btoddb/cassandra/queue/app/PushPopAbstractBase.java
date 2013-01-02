package com.btoddb.cassandra.queue.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PushPopAbstractBase implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(PushPopAbstractBase.class);

    private QueueProperties envProps;
    private String delayPropName;
    private int numMsgsToProcess;
    private int msgsProcessed;
    private volatile boolean stopProcessing = false;

    private Thread theThread;
    long start;
    long end = -1;

    public PushPopAbstractBase(QueueProperties envProps, String delayPropName) {
        this.envProps = envProps;
        this.delayPropName = delayPropName;
    }

    public void start(int numMsgsToPush, int threadNum) {
        this.numMsgsToProcess = numMsgsToPush;
        theThread = new Thread(this);
        theThread.setDaemon(true);
        theThread.setName(getClass().getSimpleName()+"-"+threadNum);
        theThread.start();
    }

    protected abstract boolean processMsg();

    protected abstract void shutdownAndWait();

    @Override
    public void run() {
        start = System.currentTimeMillis();

        msgsProcessed = 0;
        while (msgsProcessed < numMsgsToProcess && !stopProcessing) {
            try {
                if (processMsg()) {
                    msgsProcessed++;
                }
            }
            catch (Throwable e) {
                logger.error("exception while processing messages", e);
            }

            long delay = envProps.getPropertyAsLong(delayPropName, 0L);
            if (0 < delay) {
                try {
                    Thread.sleep(delay);
                }
                catch (InterruptedException e) {
                    logger.debug("Interrupted while sleeping. Shutting down?");
                    // ignore
                }
            }

        }

        try {
            shutdownAndWait();
        }
        catch (Exception e) {
            logger.error("exception while shutting down " + this.getClass().getSimpleName(), e);
        }

        end = System.currentTimeMillis();

        logger.info(this.getClass().getSimpleName() + " " + msgsProcessed + " msgs");
    }

    public int getNumMsgsToProcess() {
        return numMsgsToProcess;
    }

    public int getMsgsProcessed() {
        return msgsProcessed;
    }

    public long getElapsedTime() {
        if (-1 == end) {
            return System.currentTimeMillis() - start;
        }
        else {
            return end - start;
        }
    }

    public boolean isThreadAlive() {
        return theThread.isAlive();
    }

    public boolean isFinished() {
        return msgsProcessed == numMsgsToProcess;
    }

    /**
     * This method does not block. Parent thread can call
     * {@link #isThreadAlive()} to check status.
     */
    public void shutdown() {
        // do not block - parent will call isThreadAlive
        stopProcessing = true;
        Thread.interrupted();
    }

}
