package com.real.cassandra.queue;

import java.io.Serializable;

import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.hom.annotations.Column;
import com.real.hom.annotations.Entity;
import com.real.hom.annotations.Id;
import com.real.hom.annotations.Table;

@Entity
@Table(QueueRepositoryImpl.QUEUE_DESCRIPTORS_COLFAM)
public class QueueDescriptor implements Descriptor {
    @Id
    @Column("name")
    private String name;
    
    @Column("maxPushTimePerPipe")
    private long maxPushTimePerPipe;
    
    @Column("maxPushesPerPipe")
    private int maxPushesPerPipe;
    
    @Column("transactionTimeout")
    private long transactionTimeout;

    @Override
    public Serializable getId() {
        return getName();
    }

    public QueueDescriptor() {
    }
    
    public QueueDescriptor(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setMaxPushTimePerPipe(long maxPushTimePerPipe) {
        this.maxPushTimePerPipe = maxPushTimePerPipe;
    }

    public long getMaxPushTimePerPipe() {
        return maxPushTimePerPipe;
    }

    public void setMaxPushesPerPipe(int maxPushesPerPipe) {
        this.maxPushesPerPipe = maxPushesPerPipe;
    }

    public int getMaxPushesPerPipe() {
        return maxPushesPerPipe;
    }

    public long getTransactionTimeout() {
        return transactionTimeout;
    }

    public void setTransactionTimeout(long transactionTimeout) {
        this.transactionTimeout = transactionTimeout;
    }

    public void setName(String name) {
        this.name = name;
    }

}
