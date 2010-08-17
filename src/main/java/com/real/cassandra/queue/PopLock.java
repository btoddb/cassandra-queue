package com.real.cassandra.queue;

import java.util.concurrent.locks.LockSupport;

public interface PopLock {

    /**
     * straight from the javadoc for {@link LockSupport}.
     */
    void lock();

    void unlock();

}
