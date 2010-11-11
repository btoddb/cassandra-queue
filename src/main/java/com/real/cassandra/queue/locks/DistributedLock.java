package com.real.cassandra.queue.locks;

import org.wyki.zookeeper.cages.ZkWriteLock;

/**
 * Adapts a {@link ZkWriteLock} to implement a common {@link Lock} interface.
 *
 * @author Andrew Ebaugh
 * @version $Id: DistributedLock.java,v 1.1 2010/10/29 20:33:03 aebaugh Exp $
 */
public class DistributedLock extends ZkWriteLock implements Lock {

    public DistributedLock(String lockPath) {
        super(lockPath);
    }
}
