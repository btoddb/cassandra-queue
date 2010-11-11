package com.real.cassandra.queue.utils;

import java.util.Date;
import java.util.LinkedList;

public class RollingStat implements Runnable {
    public static final long NEVER_REMOVE = -1;

    private LinkedList<StatSample> sampleQueue = new LinkedList<StatSample>();
    private long windowSizeInMillis;

    private long totalSamplesProcessed = 0;
    private long numSamplesInWindow = 0;

    private double sumOfValues = 0;
    private double avgOfValues = 0;

    private Object dataMonitor = new Object();

    private boolean stopProcessing = false;
    private Thread theThread;

    public RollingStat(long windowSizeInMillis) {
        this.windowSizeInMillis = windowSizeInMillis;
        theThread = new Thread(this);
        theThread.setDaemon(true);
        theThread.setName(this.getClass().getSimpleName());
        theThread.start();
    }

    public void run() {
        while (!stopProcessing) {
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                // ignore
            }
            if (-1 != windowSizeInMillis) {
                pruneOldSamples(windowSizeInMillis);
            }
        }
    }

    public void addSample(long value) {
        synchronized (dataMonitor) {
            sampleQueue.add(new StatSample(value));

            totalSamplesProcessed++;
            numSamplesInWindow++;
            sumOfValues += value;
            avgOfValues = (double) sumOfValues / (double) numSamplesInWindow;
        }
    }

    public long getLatestTimestamp() {
        synchronized (dataMonitor) {
            StatSample sample = sampleQueue.peekLast();
            if (null != sample) {
                return sample.getTimestamp();
            }
            else {
                return 0;
            }
        }
    }

    public long getOldestTimestamp() {
        synchronized (dataMonitor) {
            StatSample sample = sampleQueue.peekFirst();
            if (null != sample) {
                return sample.getTimestamp();
            }
            else {
                return 0;
            }
        }
    }

    public double getSamplesPerMilli() {
        synchronized (dataMonitor) {
            StatSample sample = sampleQueue.peek();

            if (null == sample) {
                return 0;
            }

            long duration = System.currentTimeMillis() - sample.getTimestamp();

            return (double) numSamplesInWindow / (double) duration;

        }
    }

    public double getSamplesPerSecond() {
        return getSamplesPerMilli() * 1000.0;
    }

    public void pruneOldSamples(long olderThanInMillis) {
        for (;;) {
            long cutoffTime = new Date().getTime() - olderThanInMillis;
            synchronized (dataMonitor) {
                StatSample sample = sampleQueue.peek();

                if (null != sample && sample.getTimestamp() < cutoffTime) {
                    sampleQueue.remove();

                    numSamplesInWindow--;
                    sumOfValues -= sample.getValue();
                    avgOfValues = (double) sumOfValues / (double) numSamplesInWindow;
                }
                else {
                    break;
                }
            }
        }
    }

    // public Snapshot getSnapshot() {
    // synchronized (dataMonitor) {
    // return new Snapshot(numSamples, numErrored, sumOfValues,
    // sumOfSuccessfulTotalTestDuration,
    // sumOfErroredMillisPerOp, sumOfErroredTotalTestDuration);
    // }
    // }

    public void setWindowSizeInMillis(long windowSizeInMillis) {
        this.windowSizeInMillis = windowSizeInMillis;
    }

    public void shutdown() {
        stopProcessing = true;
        theThread.interrupt();
    }

    public long getNumSamplesInWindow() {
        return numSamplesInWindow;
    }

    public double getSumOfValues() {
        return sumOfValues;
    }

    public double getAvgOfValues() {
        return avgOfValues;
    }

    public long getTotalSamplesProcessed() {
        return totalSamplesProcessed;
    }

    // public class Snapshot {
    // private long numSuccessful;
    // private long numErrored;
    // private double sumOfSuccessfulMillisPerOp;
    // private double sumOfSuccessfulTotalTestDuration;
    // private double sumOfErroredMillisPerOp;
    // private double sumOfErroredTotalTestDuration;
    //
    // public Snapshot(long numSuccessful, long numErrored, double
    // sumOfSuccessfulMillisPerOp,
    // double sumOfSuccessfulTotalTestDuration, double sumOfErroredMillisPerOp,
    // double sumOfErroredTotalTestDuration) {
    // this.numSuccessful = numSuccessful;
    // this.numErrored = numErrored;
    // this.sumOfSuccessfulMillisPerOp = sumOfSuccessfulMillisPerOp;
    // this.sumOfSuccessfulTotalTestDuration = sumOfSuccessfulTotalTestDuration;
    // this.sumOfErroredMillisPerOp = sumOfErroredMillisPerOp;
    // this.sumOfErroredTotalTestDuration = sumOfErroredTotalTestDuration;
    // }
    //
    // public long getNumSuccessful() {
    // return numSuccessful;
    // }
    //
    // public long getNumErrored() {
    // return numErrored;
    // }
    //
    // public double getSumOfSuccessfulMillisPerOp() {
    // return sumOfSuccessfulMillisPerOp;
    // }
    //
    // public double getSumOfSuccessfulTotalTestDuration() {
    // return sumOfSuccessfulTotalTestDuration;
    // }
    //
    // public double getSumOfErroredMillisPerOp() {
    // return sumOfErroredMillisPerOp;
    // }
    //
    // public double getSumOfErroredTotalTestDuration() {
    // return sumOfErroredTotalTestDuration;
    // }
    // }

}
