package com.real.cassandra.queue.locks;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a solution for grabbing access to a mutual exclusive section.
 * Clients call {@link #lock(Object)} to gain access and
 * {@link #release(PipeDescriptorImpl)} when leaving critical section. Objects
 * passed as the key into the lock map must have equals and hashCode properly
 * defined.
 * 
 * @author Todd Burruss
 */
public class LocalLockerImpl {
    private static Logger logger = LoggerFactory.getLogger(LocalLockerImpl.class);

    private Map<Object, AtomicBoolean> lockObjMap = Collections.synchronizedMap(new HashMap<Object, AtomicBoolean>());
    private Object mapMonObj = new Object();

    public boolean lock(Object obj) {
        AtomicBoolean tmp = lockObjMap.get(obj);
        if (null == tmp) {
            synchronized (mapMonObj) {
                tmp = lockObjMap.get(obj);
                if (null == tmp) {
                    tmp = new AtomicBoolean(true);
                    lockObjMap.put(obj, tmp);
                    return true;
                }
            }
        }
        boolean result = tmp.compareAndSet(false, true);
        logger.debug("compareAndSet result for object, {} : {}", obj.toString(), result);
        return result;
    }

    public boolean lock(Object qName, int lockRetryLimit, long lockRetryDelay) {
        for (int i = 0; i < lockRetryLimit; i++) {
            if (lock(qName)) {
                return true;
            }
            try {
                Thread.sleep(lockRetryDelay);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                // do nothing else
            }
        }

        return false;
    }

    public void release(Object obj) {
        if (null != obj) {
            lockObjMap.remove(obj);
            logger.debug("removed lock for object, {}", obj.toString());
            // AtomicBoolean tmp = pipeUseLockMap.get(pipeDesc);
            // if (null != tmp) {
            // if (!tmp.compareAndSet(true, false)) {
            // logger.error("synchronization error - the AtomicBoolean should have been true, but was not.  this means that two threads had the lock : pipeDesc = "
            // + pipeDesc.toString());
            // }
            // }
        }
    }

}
