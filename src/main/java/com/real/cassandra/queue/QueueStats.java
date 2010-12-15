package com.real.cassandra.queue;

import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.hom.annotations.Column;
import com.real.hom.annotations.Entity;
import com.real.hom.annotations.Id;
import com.real.hom.annotations.Table;

/**
 * Model object for queue statistics.
 * 
 * @author Todd Burruss
 */
@Entity
@Table(QueueRepositoryImpl.QUEUE_STATS_COLFAM)
public class QueueStats {
    @Id
    private String queueName;
    
    @Column("totalPushes")
    private long totalPushes;
    
    @Column("totalPops")
    private long totalPops;
    
    @Column("recentPushesPerSec")
    private double recentPushesPerSec;
    
    @Column("recentPopsPerSec")
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
