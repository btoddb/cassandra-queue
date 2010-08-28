package com.real.cassandra.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PushPopAbstractBase implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(PushPopAbstractBase.class);

    CassQueue cq;
    private EnvProperties envProps;
    private String delayPropName;
    private String baseValue;
    private int numMsgsToProcess;
    private int msgsProcessed;
    private boolean stopProcessing = false;

    TestUtils testUtils;

    private Thread theThread;
    long start;
    long end = -1;

    public PushPopAbstractBase(CassQueue cq, String baseValue, EnvProperties envProps, String delayPropName) {
        this.cq = cq;
        this.baseValue = baseValue;
        this.envProps = envProps;
        this.delayPropName = delayPropName;
        this.testUtils = new TestUtils(cq);
    }

    public void start(int numMsgsToPush) {
        this.numMsgsToProcess = numMsgsToPush;
        theThread = new Thread(this);
        theThread.setDaemon(true);
        theThread.setName(getClass().getSimpleName());
        theThread.start();
    }

    protected abstract boolean processMsg(String value) throws Exception;

    @Override
    public void run() {
        start = System.currentTimeMillis();

        msgsProcessed = 0;
        while (msgsProcessed < numMsgsToProcess && !stopProcessing) {
            try {
                if (processMsg(testUtils.formatMsgValue(baseValue, msgsProcessed))) {
                    msgsProcessed++;
                }
            }
            catch (Exception e) {
                logger.error("exception while processing messages", e);
            }

            long delay = envProps.getPropertyAsLong(delayPropName, 0L);
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

    public String getBaseValue() {
        return baseValue;
    }

    public void setStopProcessing(boolean stopProcessing) {
        this.stopProcessing = stopProcessing;
    }

}
