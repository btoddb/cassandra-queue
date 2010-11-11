package com.real.cassandra.queue.locks;

/**
 * Manager interface for generating locks on particular objects (or what they represent). If a lock can be obtained on
 * the passed in object, an {@link ObjectLock} instance is returned. This can then be used to release the lock on the
 * object.
 *
 * The shutdownAndWait method is exposed to allow stateful locking mechanisms to clean up on shutdown.
 *
 * @author Andrew Ebaugh
 * @version $Id: Locker.java,v 1.1 2010/10/29 20:33:03 aebaugh Exp $
 */
public interface Locker<I> {


    ObjectLock<I> lock(I object);

    ObjectLock<I> lock(I object, int lockRetryLimit, long lockRetryDelay);

    void release(ObjectLock<I> objectLock);

    void shutdownAndWait();

}
