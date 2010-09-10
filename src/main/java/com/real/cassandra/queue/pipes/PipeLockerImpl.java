package com.real.cassandra.queue.pipes;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeLockerImpl {
    private static Logger logger = LoggerFactory.getLogger(PipeLockerImpl.class);

    private Map<PipeDescriptorImpl, AtomicBoolean> pipeUseLockMap = new HashMap<PipeDescriptorImpl, AtomicBoolean>();
    private Object mapMonObj = new Object();

    // TODO:BTB must cleanup this map when pipes are finished and empty,
    // otherwise memory leak

    public boolean lock(PipeDescriptorImpl pipeDesc) {
        AtomicBoolean tmp = pipeUseLockMap.get(pipeDesc);
        if (null == tmp) {
            synchronized (mapMonObj) {
                tmp = pipeUseLockMap.get(pipeDesc);
                if (null == tmp) {
                    tmp = new AtomicBoolean(true);
                    pipeUseLockMap.put(pipeDesc, tmp);
                    return true;
                }
            }
        }
        return tmp.compareAndSet(false, true);
    }

    public void release(PipeDescriptorImpl pipeDesc) {
        if (null != pipeDesc) {
            AtomicBoolean tmp = pipeUseLockMap.get(pipeDesc);
            if (null != tmp) {
                if (!tmp.compareAndSet(true, false)) {
                    logger.error("synchronization error - the AtomicBoolean should have been true, but was not.  this means that two threads had the lock : pipeDesc = "
                            + pipeDesc.toString());
                }
            }
        }
    }

    public void shutdown() {
        // do nothing
    }

}
