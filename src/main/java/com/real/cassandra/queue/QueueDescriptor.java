package com.real.cassandra.queue;

public class QueueDescriptor {
    private String name;
    private long maxPushTimeOfPipe;
    private int maxPushesPerPipe;
    private int maxPopWidth;
    private long popPipeRefreshDelay;

    public QueueDescriptor(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setMaxPushTimeOfPipe(long maxPushTimeOfPipe) {
        this.maxPushTimeOfPipe = maxPushTimeOfPipe;
    }

    public long getMaxPushTimeOfPipe() {
        return maxPushTimeOfPipe;
    }

    public void setMaxPushesPerPipe(int maxPushesPerPipe) {
        this.maxPushesPerPipe = maxPushesPerPipe;
    }

    public int getMaxPushesPerPipe() {
        return maxPushesPerPipe;
    }

    public void setMaxPopWidth(int maxPopWidth) {
        this.maxPopWidth = maxPopWidth;
    }

    public int getMaxPopWidth() {
        return maxPopWidth;
    }

    public long getPopPipeRefreshDelay() {
        return popPipeRefreshDelay;
    }

    public void setPopPipeRefreshDelay(long popPipeRefreshDelay) {
        this.popPipeRefreshDelay = popPipeRefreshDelay;
    }

}
