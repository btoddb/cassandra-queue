package com.real.cassandra.queue.locks;

/**
 * Interface that should be used and adapted to by various lock mechanisms to give us a common way to abstract them.
 * This only implements the basic methods needed to support various libraries. Since a typed version of the lock with
 * specific acquire methods exposed, we really only need the release() method defined, so we can call it when
 * using the instance as a generic type.
 *
 * @author Andrew Ebaugh
 * @version $Id: Lock.java,v 1.2 2011/01/27 22:44:17 aebaugh Exp $
 */
public interface Lock {

	void release();

}
