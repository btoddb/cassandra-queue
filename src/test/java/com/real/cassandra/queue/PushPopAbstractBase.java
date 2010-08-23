package com.real.cassandra.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PushPopAbstractBase implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(PushPopAbstractBase.class);

    CassQueue cq;
    private long delay;
    private String baseValue;
    private int numMsgsToProcess;
    private int msgsProcessed;

    TestUtils testUtils;

    private Thread theThread;
    long start;
    long end = -1;

    public PushPopAbstractBase(CassQueue cq, String baseValue) {
        this.cq = cq;
        this.baseValue = baseValue;
        this.testUtils = new TestUtils(cq);
    }

    public void start(int numMsgsToPush, long delay) {
        this.numMsgsToProcess = numMsgsToPush;
        this.delay = delay;
        theThread = new Thread(this);
        theThread.start();
    }

    protected abstract boolean processMsg(String value) throws Exception;

    @Override
    public void run() {
        start = System.currentTimeMillis();

        msgsProcessed = 0;
        while (msgsProcessed < numMsgsToProcess) {
            try {
                if (processMsg(testUtils.formatMsgValue(baseValue, msgsProcessed))) {
                    msgsProcessed++;
                }
            }
            catch (Exception e) {
                logger.error("exception while processing messages", e);
            }

            if (0 < delay) {
                try {
                    Thread.sleep(delay);
                }
                catch (InterruptedException e) {
                    // ignore
                }
            }

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

    public boolean isFinished() {
        return msgsProcessed == numMsgsToProcess;
    }

    public CassQueue getCassQueue() {
        return cq;
    }

    public long getDelay() {
        return delay;
    }

    public String getBaseValue() {
        return baseValue;
    }

}
