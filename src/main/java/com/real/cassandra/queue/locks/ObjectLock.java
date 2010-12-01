package com.real.cassandra.queue.locks;

/**
 * Represents a locked object. Generic so it can be used to represent a lock on any type of object, the lock field then
 * represents that exclusive access is given to the object, or whatever it represents. 
 *
 * @author Andrew Ebaugh
 * @version $Id: ObjectLock.java,v 1.1 2010/10/29 20:33:03 aebaugh Exp $
 */
public class ObjectLock<I> {

    private I lockedObj;
    private Lock lock;

    public ObjectLock(I lockedObj, Lock lock) {
        this.lockedObj = lockedObj;
        this.lock = lock;
    }

    public I getLockedObj() {
        return lockedObj;
    }

    public Lock getLock() {
        return lock;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !getClass().equals(o.getClass())) return false;

        ObjectLock<?> that = (ObjectLock<?>) o;

        if (lockedObj != null ? !lockedObj.equals(that.lockedObj) : that.lockedObj != null) return false;
        if (lock != null ? !lock.equals(that.lock) : that.lock != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = lockedObj != null ? lockedObj.hashCode() : 0;
        result = 31 * result + (lock != null ? lock.hashCode() : 0);
        return result;
    }
}
