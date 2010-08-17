package com.real.cassandra.queue.spring;

import org.springframework.beans.factory.FactoryBean;

import com.real.cassandra.queue.CassQueue;
import com.real.cassandra.queue.repository.QueueRepository;

/**
 * Spring factory bean used to create {@link CassQueue} type queues.
 * 
 * @author Todd Burruss
 */
public class QueueFactoryBean implements FactoryBean<CassQueue> {
    private QueueRepository queueRepository;
    private String qName;
    private int numPipes;
    private boolean popLocks = true;
    private boolean distributed = false;

    @Override
    public CassQueue getObject() throws Exception {
        queueRepository.createQueue(qName, numPipes);
        return new CassQueue(queueRepository, qName, numPipes, popLocks, distributed);
    }

    @Override
    public Class<?> getObjectType() {
        return CassQueue.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setQueueRepository(QueueRepository queueRepository) {
        this.queueRepository = queueRepository;
    }

    public void setqName(String qName) {
        this.qName = qName;
    }

    public void setNumPipes(int numPipes) {
        this.numPipes = numPipes;
    }

    public void setPopLocks(boolean popLocks) {
        this.popLocks = popLocks;
    }

    public void setDistributed(boolean distributed) {
        this.distributed = distributed;
    }

}
