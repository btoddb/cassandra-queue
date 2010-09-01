package com.real.cassandra.queue.locking;

import java.util.concurrent.locks.LockSupport;

public interface PopLock {

    /**
     * straight from the javadoc for {@link LockSupport}.
     */
    void lock(int pipeNum);

    void unlock(int pipeNum);

}
