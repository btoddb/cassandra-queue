package com.real.cassandra.queue;

/**
 * Does nothing, used as a No Op call.
 * 
 * @author Todd Burruss
 */
public class PopLockNoOpImpl implements PopLock {

    @Override
    public void lock() {
        // do nothing
    }

    @Override
    public void unlock() {
        // do nothing
    }

}
