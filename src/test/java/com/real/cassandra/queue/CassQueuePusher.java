package com.real.cassandra.queue;

public class CassQueuePusher implements Runnable {
    private CassQueue cq;
    private int numMsgsToPush;
    private int msgsPushed;
    private long delay;
    private String baseValue;
    private TestUtils testUtils;

    private Thread theThread;

    public CassQueuePusher(CassQueue cq, String baseValue) {
        this.cq = cq;
        this.baseValue = baseValue;
        testUtils = new TestUtils(cq);
    }

    @Override
    public void run() {
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

        System.out.println("popped " + msgsPushed + " msgs");
    }

    public void start(int numMsgsToPush, long delay) {
        this.numMsgsToPush = numMsgsToPush;
        this.delay = delay;
        theThread = new Thread(this);
        theThread.start();
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
