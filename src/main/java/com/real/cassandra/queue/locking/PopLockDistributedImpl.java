package com.real.cassandra.queue.locking;

import org.wyki.zookeeper.cages.ZkCagesException;
import org.wyki.zookeeper.cages.ZkWriteLock;

public class PopLockDistributedImpl implements PopLock {
    private static final String DISTRIBUTED_LOCK_PATH = "/queues/";

    private final String queueName;
    // private final int numPipes;
    private final ZkWriteLock lock;

    public PopLockDistributedImpl(String queueName, int numPipes) {
        this.queueName = queueName;
        // this.numPipes = numPipes;
        lock = new ZkWriteLock(DISTRIBUTED_LOCK_PATH + this.queueName);
    }

    @Override
    public void lock(int pipeNum) {
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
    public void unlock(int pipeNum) {
        lock.release();
    }

}
