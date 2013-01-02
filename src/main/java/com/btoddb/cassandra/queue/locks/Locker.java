package com.btoddb.cassandra.queue.locks;

/**
 * Manager interface for generating locks on particular objects (or what they represent). If a lock can be obtained on
 * the passed in object, an {@link ObjectLock} instance is returned. This can then be used to release the lock on the
 * object.
 *
 * The shutdownAndWait method is exposed to allow stateful locking mechanisms to clean up on shutdown.
 *
 */
public interface Locker<I> {

    ObjectLock<I> lock(I object);

    ObjectLock<I> lock(I object, int lockRetryLimit, long lockRetryDelay);

    void release(ObjectLock<I> objectLock);

    void shutdownAndWait();

    int getLockCountSuccess();

    int getReleaseCount();
}
