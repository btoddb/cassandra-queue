package com.btoddb.cassandra.queue;


import com.btoddb.cassandra.queue.repository.QueueRepositoryImpl;
import me.prettyprint.hom.annotations.Column;
import me.prettyprint.hom.annotations.Id;

import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name=QueueRepositoryImpl.QUEUE_DESCRIPTORS_COLFAM)
public class QueueDescriptor implements Descriptor {
    @Id
    private String name;
    
    @Column(name = "maxPushTimePerPipe")
    private long maxPushTimePerPipe;
    
    @Column(name = "maxPushesPerPipe")
    private int maxPushesPerPipe;
    
    @Column(name = "transactionTimeout")
    private long transactionTimeout;

    public QueueDescriptor() {
    }
    
    public QueueDescriptor(String name) {
        this.name = name;
    }

    @Override
    public String getId() {
        return getName();
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
