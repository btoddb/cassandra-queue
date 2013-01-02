package com.btoddb.cassandra.queue.locks;

/**
 * Interface that should be used and adapted to by various lock mechanisms to give us a common way to abstract them.
 * This only implements the basic methods needed to support various libraries. Since a typed version of the lock with
 * specific acquire methods exposed, we really only need the release() method defined, so we can call it when
 * using the instance as a generic type.
 *
 */
public interface Lock {

	void release();

}
