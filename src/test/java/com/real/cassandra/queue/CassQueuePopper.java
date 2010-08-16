package com.real.cassandra.queue;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class CassQueuePopper implements Runnable {

    private CassQueue cq;
    private int numMsgsToPop;
    private int msgsPopped;
    private long delay;
    private String baseValue;
    @SuppressWarnings("unused")
    private TestUtils testUtils;
    private List<CassQMsg> popList = Collections.synchronizedList(new LinkedList<CassQMsg>());

    private Thread theThread;

    public CassQueuePopper(CassQueue cq, String baseValue) {
        this.cq = cq;
        this.baseValue = baseValue;
        testUtils = new TestUtils(cq);
    }

    @Override
    public void run() {
        while (msgsPopped < numMsgsToPop) {
            try {
                CassQMsg qMsg = cq.pop();
                if (null != qMsg) {
                    popList.add(qMsg);
                    msgsPopped++;
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

        System.out.println("popped " + msgsPopped + " msgs");
    }

    public void start(int numMsgsToPop, long delay) {
        this.numMsgsToPop = numMsgsToPop;
        this.delay = delay;
        theThread = new Thread(this);
        theThread.start();
    }

    public List<CassQMsg> getPopList() {
        return popList;
    }

    public boolean isFinished() {
        return msgsPopped == numMsgsToPop;
    }

    public int getNumMsgsToPop() {
        return numMsgsToPop;
    }

    public void setNumMsgsToPop(int numMsgsToPop) {
        this.numMsgsToPop = numMsgsToPop;
    }

    public CassQueue getCassQueue() {
        return cq;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public String getBaseValue() {
        return baseValue;
    }

    public int getMsgsPopped() {
        return msgsPopped;
    }
}
