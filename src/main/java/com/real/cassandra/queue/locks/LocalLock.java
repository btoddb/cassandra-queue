package com.real.cassandra.queue.locks;

/**
 * Implements a local {@link Lock} interface, that doesn't do anything, but can be used to represent an object lock.
 *
 * @author Andrew Ebaugh
 * @version $Id: LocalLock.java,v 1.1 2010/10/29 20:33:03 aebaugh Exp $
 */
public class LocalLock implements Lock {

    @Override
    public void release() {
        //do nothing
    }
}
