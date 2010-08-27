package com.real.cassandra.queue;

public class QueueDescriptor {
    private String name;
    private int numPipes;
    private long pushStartPipe;
    private long popStartPipe;

    public QueueDescriptor(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int getNumPipes() {
        return numPipes;
    }

    public long getPushStartPipe() {
        return pushStartPipe;
    }

    public void setNumPipes(int numPipes) {
        this.numPipes = numPipes;
    }

    public void setPushStartPipe(long startPipe) {
        this.pushStartPipe = startPipe;
    }

    public long getPopStartPipe() {
        return popStartPipe;
    }

    public void setPopStartPipe(long popStartPipe) {
        this.popStartPipe = popStartPipe;
    }

}
