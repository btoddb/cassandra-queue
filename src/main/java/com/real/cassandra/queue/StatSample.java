package com.real.cassandra.queue;

public class StatSample {
    private long value;
    private long timestamp;

    public StatSample(long value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public StatSample(long value) {
        this(value, System.currentTimeMillis());
    }

    public long getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

}
