package com.real.cassandra.queue;

/**
 * Model object for queue statistics.
 * 
 * @author Todd Burruss
 */
public class QueueStats {
    private String qName;
    private long totalPushes;
    private long totalPops;
    private double recentPushesPerSec;
    private double recentPopsPerSec;

    public QueueStats(String qName) {
        this.qName = qName;
    }

    public long getTotalPushes() {
        return totalPushes;
    }

    public void setTotalPushes(long totalPushes) {
        this.totalPushes = totalPushes;
    }

    public long getTotalPops() {
        return totalPops;
    }

    public void setTotalPops(long totalPops) {
        this.totalPops = totalPops;
    }

    public double getRecentPushesPerSec() {
        return recentPushesPerSec;
    }

    public void setRecentPushesPerSec(double recentPushesPerSec) {
        this.recentPushesPerSec = recentPushesPerSec;
    }

    public double getRecentPopsPerSec() {
        return recentPopsPerSec;
    }

    public void setRecentPopsPerSec(double recentPopsPerSec) {
        this.recentPopsPerSec = recentPopsPerSec;
    }

    public String getQName() {
        return qName;
    }

    public void incTotalPushes(int count) {
        totalPushes += count;
    }

    public void incTotalPops(int count) {
        totalPops += count;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("QueueStats [qName=");
        builder.append(qName);
        builder.append(", totalPushes=");
        builder.append(totalPushes);
        builder.append(", totalPops=");
        builder.append(totalPops);
        builder.append(", recentPushesPerSec=");
        builder.append(recentPushesPerSec);
        builder.append(", recentPopsPerSec=");
        builder.append(recentPopsPerSec);
        builder.append("]");
        return builder.toString();
    }
}
