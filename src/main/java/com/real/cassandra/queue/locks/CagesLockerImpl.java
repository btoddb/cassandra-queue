package com.real.cassandra.queue.locks;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wyki.zookeeper.cages.ZkCagesException;
import org.wyki.zookeeper.cages.ZkSessionManager;

import com.real.cassandra.queue.Descriptor;

/**
 * Distributed version of {@link LocalLockerImpl} that uses Zookeeper/cages to gain mutex over
 * shared lock paths, and perform operations on data values those values symbolically represent.
 *
 * Clients call {@link #lock(com.real.cassandra.queue.Descriptor)} to gain access and
 * {@link #release(ObjectLock)} when leaving critical section.
 *
 * @author Todd Burruss
 * @author Andrew Ebaugh
 */
public class CagesLockerImpl<I extends Descriptor> implements Locker<I> {
    private static Logger logger = LoggerFactory.getLogger(CagesLockerImpl.class);

    private String lockPath;
    private ZkSessionManager sessionManager;
    private String connectString;
    private Integer sessionTimeout;
    private Integer maxConnectAttempts = 10;

    private AtomicInteger lockCount = new AtomicInteger(0);
    private AtomicInteger releaseCount = new AtomicInteger(0);

    public CagesLockerImpl(String lockPath, String connectString, Integer sessionTimeout, Integer maxConnectAttempts)
            throws IOException, ExecutionException, InterruptedException {

        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
        this.lockPath = lockPath;
        this.maxConnectAttempts = maxConnectAttempts;

        ZkSessionManager.initializeInstance(connectString, sessionTimeout, maxConnectAttempts);
        sessionManager = ZkSessionManager.instance();

        if(sessionManager == null) {
            logger.error("No session manager created, connect string {} and lock path", connectString, lockPath);
            throw new IllegalStateException("Zookeeper session manager was null after initialization");
        }
    }

    @Override
    public ObjectLock<I> lock(I object) {
        DistributedLock writeLock = new DistributedLock(lockPath, object.getId().toString(), 2*sessionTimeout);
        ObjectLock<I> lock = null;

        try {
            if(writeLock.tryAcquire()) {
                lock = new ObjectLock<I>(object, writeLock);
                logger.debug("Acquired lock for object {}",object.getId());
                lockCount.incrementAndGet();
            }
        }
        catch (ZkCagesException e) {
            if(e.getKeeperException() != null) {
                logger.warn(String.format("Lock acquire for id %s failed with error code %s and zookeeper exception %s",
                        object.getId(), e.getErrorCode().name(), e.getKeeperException().getMessage()), e.getKeeperException());
            }
            else {
                logger.warn(String.format("Lock acquire for id %s failed with error code %s",
                        object.getId(), e.getErrorCode().name()), e);
            }
        }
        catch (InterruptedException e) {
            logger.warn("Interrupted trying to acquire lock for object {}", object.getId());
            //clear thread interrupt flag
            Thread.interrupted();
        }

        return lock;
    }

    @Override
    public ObjectLock<I> lock(I object, int lockRetryLimit, long lockRetryDelay) {
        ObjectLock<I> lock = null;

        for (int i = 0; i < lockRetryLimit && lock == null; i++) {
            lock = lock(object);
            if (lock == null) {
                try {
                    Thread.sleep(lockRetryDelay);
                }
                catch (InterruptedException e) {
                    logger.warn("Interrupted sleeping for lock retry.");
                    //clear thread interrupt flag
                    Thread.interrupted();
                }
            }
        }

        return lock;
    }

    @Override
    public void release(ObjectLock<I> objectLock) {
        if(objectLock != null) {
            logger.debug("Released lock for object {}",objectLock.getLockedObj().getId());
            objectLock.getLock().release();
            releaseCount.incrementAndGet();
        }
    }

    @Override
    public void shutdownAndWait() {
        try {
            sessionManager.shutdown();
        } catch (InterruptedException e) {
            logger.warn("Interrupted shutting down Zookeeper session.");
            //clear thread interrupt flag
            Thread.interrupted();
        }
    }


    public int getLockCount() {
        return lockCount.get();
    }

    public int getReleaseCount() {
        return releaseCount.get();
    }

    public String getConnectString() {
        return connectString;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    public Integer getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(Integer sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public Integer getMaxConnectAttempts() {
        return maxConnectAttempts;
    }

    public void setMaxConnectAttempts(Integer maxConnectAttempts) {
        this.maxConnectAttempts = maxConnectAttempts;
    }
}
