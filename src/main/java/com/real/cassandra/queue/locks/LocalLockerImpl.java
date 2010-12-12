package com.real.cassandra.queue.locks;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.Descriptor;

/**
 * Provides a solution for grabbing access to a mutual exclusive section.
 * Clients call {@link #lock(com.real.cassandra.queue.Descriptor)} to gain access and
 * {@link #release(ObjectLock)} when leaving critical section. UUID objects
 * passed as the key into the lock map must have equals and hashCode properly
 * defined.
 * 
 * @author Todd Burruss
 */
public class LocalLockerImpl<I extends Descriptor> implements Locker<I> {
    private static Logger logger = LoggerFactory.getLogger(LocalLockerImpl.class);

    //represents the acquired lock instance, not useful in this implementation
    private final static LocalLock LOCK = new LocalLock();

    final private Map<Object, ObjectLock<I>> lockObjMap =
            Collections.synchronizedMap(new HashMap<Object, ObjectLock<I>>());

    final private Object mapMonObj = new Object();

    private AtomicInteger lockCount = new AtomicInteger(0);
    private AtomicInteger releaseCount = new AtomicInteger(0);

    @Override
    public ObjectLock<I> lock(I object) {
        ObjectLock<I> lock = null;
            synchronized (mapMonObj) {
                ObjectLock<I> existingLock = lockObjMap.get(object.getId());
                if (null == existingLock) {
                    lock = new ObjectLock<I>(object, LOCK);
                    lockObjMap.put(object.getId(), lock);
                    lockCount.incrementAndGet();
                }
            }

        logger.debug("lock acquire result for object {} : {}", object.getId().toString(), (lock != null));
        return lock;
    }

    @Override
    public ObjectLock<I> lock(I object, int lockRetryLimit, long lockRetryDelay) {
        ObjectLock<I> lock = null;

        for (int i = 0; i < lockRetryLimit; i++) {
            lock = lock(object);
            if (lock != null) {
                break;
            }
            try {
                Thread.sleep(lockRetryDelay);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                // do nothing else
            }
        }

        return lock;
    }

    @Override
    public void release(ObjectLock<I> objectLock) {
        if (null != objectLock) {
            lockObjMap.remove(objectLock.getLockedObj().getId());
            releaseCount.incrementAndGet();
            logger.debug("removed lock for object {}", objectLock.getLockedObj().getId().toString());
        }
    }


    public void shutdownAndWait() {
        // do nothing
    }

    /**
     * Map should only be used during testing to verify state.
     * 
     * @return
     */
    public Map<Object, ObjectLock<I>> getMap() {
        return lockObjMap;
    }

    public int getLockCount() {
        return lockCount.get();
    }

    public int getReleaseCount() {
        return releaseCount.get();
    }

}
