package com.btoddb.cassandra.queue.locks;

import org.apache.zookeeper.KeeperException;
import org.wyki.zookeeper.cages.ZkCagesException;
import org.wyki.zookeeper.cages.ZkResourceWriteLock;

/**
 * Adapts a {@link ZkResourceWriteLock} to implement a common {@link Lock} interface. Also adds the ability to select
 * between async vs. sync zookeeper communication.
 *
 */
public class DistributedLock extends ZkResourceWriteLock implements Lock {

	public DistributedLock(String lockPath, String resource) {
		super(lockPath, resource);
	}

    public DistributedLock(String lockPath, String resourceName, Integer maxSyncTimeout) {
        super(lockPath, resourceName, maxSyncTimeout);
    }

    /**
     * Attempts to acquire the resource lock (i.e. create resource lock path) and return false if already held.
     *
     * @return true if lock path created, otherwise false
     * @throws ZkCagesException
     * @throws InterruptedException
     */
    @Override
    public boolean tryAcquire() throws ZkCagesException, InterruptedException {
        boolean acquired;

        try {

            acquired = super.tryAcquire();
        }
        catch(ZkCagesException e) {
            if(ZkCagesException.Error.ZOOKEEPER_EXCEPTION.equals(e.getErrorCode()) && e.getKeeperException() != null &&
                    KeeperException.Code.NODEEXISTS.equals(e.getKeeperException().code())) {
                //someone already created this lock path
                acquired = false;
            }
            else {
                //some other Zk error
                throw e;
            }
        }

        return acquired;
    }
}
