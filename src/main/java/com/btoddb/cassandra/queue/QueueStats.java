package com.btoddb.cassandra.queue;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.btoddb.cassandra.queue.repository.QueueRepositoryImpl;

/**
 * Model object for queue statistics.
 * 
 * @author Todd Burruss
 */
@Entity
@Table(name = QueueRepositoryImpl.QUEUE_STATS_COLFAM)
public class QueueStats {
    @Id
    private String queueName;
    
    @Column(name = "totalPushes")
    private long totalPushes;
    
    @Column(name = "totalPops")
    private long totalPops;
    
    @Column(name = "recentPushesPerSec")
    private double recentPushesPerSec;
    
    @Column(name = "recentPopsPerSec")
    private double recentPopsPerSec;

    public QueueStats() {
    }

    public QueueStats(String queueName) {
        this.queueName = queueName;
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
        return queueName;
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
        builder.append("QueueStats [queueName=");
        builder.append(queueName);
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

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
