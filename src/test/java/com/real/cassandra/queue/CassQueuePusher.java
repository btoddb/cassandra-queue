package com.real.cassandra.queue;

public class CassQueuePusher implements Runnable {
    private CassQueue cq;
    private int numMsgsToPush;
    private int msgsPushed;
    private long delay;
    private String baseValue;
    private TestUtils testUtils;

    private Thread theThread;
    private long start;
    private long end = -1;

    public CassQueuePusher(CassQueue cq, String baseValue) {
        this.cq = cq;
        this.baseValue = baseValue;
        testUtils = new TestUtils(cq);
    }

    @Override
    public void run() {
        start = System.currentTimeMillis();
        for (int i = 0; i < numMsgsToPush; i++) {
            try {
                cq.push(testUtils.formatMsgValue(baseValue, i));
                msgsPushed++;
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

        System.out.println("pushed " + msgsPushed + " msgs");
    }

    public void start(int numMsgsToPush, long delay) {
        this.numMsgsToPush = numMsgsToPush;
        this.delay = delay;
        theThread = new Thread(this);
        theThread.start();
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
        return msgsPushed == numMsgsToPush;
    }

    public int getNumMsgsToPush() {
        return numMsgsToPush;
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

    public int getMsgsPushed() {
        return msgsPushed;
    }

}
