package com.real.cassandra.queue;

import org.wyki.zookeeper.cages.ZkCagesException;
import org.wyki.zookeeper.cages.ZkWriteLock;

public class PopLockDistributedImpl implements PopLock {
    private static final String DISTRIBUTED_LOCK_PATH = "/queues/";

    private String queueName;
    private ZkWriteLock lock;

    public PopLockDistributedImpl(String queueName) {
        this.queueName = queueName;
        lock = new ZkWriteLock(DISTRIBUTED_LOCK_PATH + this.queueName);
    }

    @Override
    public void lock() {
        try {
            lock.acquire();
        }
        catch (ZkCagesException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unlock() {
        lock.release();
    }

}
