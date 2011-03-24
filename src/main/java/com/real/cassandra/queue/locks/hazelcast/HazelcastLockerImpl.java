package com.real.cassandra.queue.locks.hazelcast;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.ILock;
import com.real.cassandra.queue.Descriptor;
import com.real.cassandra.queue.locks.Locker;
import com.real.cassandra.queue.locks.ObjectLock;

public class HazelcastLockerImpl<I extends Descriptor> implements Locker<I> {
    private static Logger logger = LoggerFactory.getLogger(HazelcastLockerImpl.class);

    private static long MAX_LOCK_WAIT_MS = 100; // 0 = don't wait

    private AtomicInteger lockCountSuccess = new AtomicInteger();
    private AtomicInteger lockCountFailure = new AtomicInteger();
    private AtomicInteger lockCountFailureWithRetry = new AtomicInteger();
    private AtomicInteger releaseCount = new AtomicInteger();
    private String domain;

    public HazelcastLockerImpl(String domain) {
        this.domain = domain;
    }

    @Override
    public ObjectLock<I> lock(I obj) {
        ILock lock = Hazelcast.getLock(createLockObject(obj.getId()));
        try {
            if (lock.tryLock(MAX_LOCK_WAIT_MS, TimeUnit.MILLISECONDS)) {
                lockCountSuccess.incrementAndGet();
                return new ObjectLock<I>(obj, new HazelcastLock(lock));
            }
        }
        catch (InterruptedException e) {
            logger.error("exception while waiting for lock acquisition", e);
        }
        lockCountFailure.incrementAndGet();
        return null;
    }

    @Override
    public ObjectLock<I> lock(I obj, int lockRetryLimit, long lockRetryDelay) {
        ObjectLock<I> objLock = null;
        for (int i = 0; i < lockRetryLimit; i++) {
            try {
                objLock = lock(obj);
            }
            catch (Throwable e) {
                logger.error("exception while locking " + createLockObject(obj), e);
            }

            if (null != objLock) {
                return objLock;
            }

            try {
                Thread.sleep(lockRetryDelay);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
            }
        }

        lockCountFailureWithRetry.incrementAndGet();
        return null;
    }

    /**
     * Hazelcast requires that the lock "key" serialize to the same byte[]
     * across the cluster. So to be safe, use strings.
     * 
     * @param obj
     * @return
     */
    private String createLockObject(Object obj) {
        return domain + "/" + obj.toString();
    }

    @Override
    public void release(ObjectLock<I> objectLock) {
        try {
            objectLock.getLock().release();
            releaseCount.incrementAndGet();
        }
        catch (Throwable e) {
            logger.error("exception while releasing", e);
        }
    }

    @Override
    public void shutdownAndWait() {
    }

    public AtomicInteger getLockCountSuccess() {
        return lockCountSuccess;
    }

    public AtomicInteger getReleaseCount() {
        return releaseCount;
    }

    public AtomicInteger getLockCountFailure() {
        return lockCountFailure;
    }

    public AtomicInteger getLockCountFailureWithRetry() {
        return lockCountFailureWithRetry;
    }

}
