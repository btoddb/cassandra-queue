package com.btoddb.cassandra.queue.locks;

/**
 * Implements a local {@link Lock} interface, that doesn't do anything, but can be used to represent an object lock.
 *
 */
public class LocalLock implements Lock {

    @Override
    public void release() {
        //do nothing
    }
}
