package com.real.cassandra.queue;

public abstract class PushPopAbstractBase implements Runnable {
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

        for (int i = 0; i < numMsgsToProcess; i++) {
            try {
                if (processMsg(testUtils.formatMsgValue(baseValue, i))) {
                    msgsProcessed++;
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            if (0 < delay) {
                try {
                    Thread.sleep(delay);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        end = System.currentTimeMillis();

        System.out.println("pushed " + msgsProcessed + " msgs");
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
