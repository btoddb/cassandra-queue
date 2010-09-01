package com.real.cassandra.queue.locking;

/**
 * Does nothing, used as a No Op call.
 * 
 * @author Todd Burruss
 */
public class PopLockNoOpImpl implements PopLock {

    @Override
    public void lock(int pipeNum) {
        // do nothing
    }

    @Override
    public void unlock(int pipeNum) {
        // do nothing
    }

}
