package com.real.cassandra.queue;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * 
 * 
 * @author Todd Burruss
 */
public class PopLockLocalImpl implements PopLock {
    private final AtomicBoolean locked = new AtomicBoolean(false);
    private final Queue<Thread> waiters = new ConcurrentLinkedQueue<Thread>();

    private final int numPipes;

    public PopLockLocalImpl(int numPipes) {
        this.numPipes = numPipes;
    }

    /**
     * straight from the javadoc for {@link LockSupport}.
     */
    @Override
    public void lock(int pipeNum) {
        boolean wasInterrupted = false;
        Thread current = Thread.currentThread();
        waiters.add(current);

        // Block while not first in queue or cannot acquire lock
        while (waiters.peek() != current || !locked.compareAndSet(false, true)) {
            LockSupport.park(this);
            if (Thread.interrupted()) {
                // ignore interrupts while waiting
                wasInterrupted = true;
            }
        }

        waiters.remove();
        if (wasInterrupted) {
            // reassert interrupt status on exit
            current.interrupt();
        }
    }

    @Override
    public void unlock(int pipeNum) {
        locked.set(false);
        LockSupport.unpark(waiters.peek());
    }

}
