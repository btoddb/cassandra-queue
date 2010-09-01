package com.real.cassandra.queue.pipeperpusher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeLockerImpl {

    // private String qName;

    private Map<PipeDescriptorImpl, AtomicBoolean> pipeUseLockMap = new HashMap<PipeDescriptorImpl, AtomicBoolean>();
    private Object mapMonObj = new Object();

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
                tmp.set(false);
            }
        }
    }

    public void shutdown() {
        // do nothing
    }

}
