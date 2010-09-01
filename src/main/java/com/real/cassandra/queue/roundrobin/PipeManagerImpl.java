package com.real.cassandra.queue.roundrobin;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.scale7.cassandra.pelops.Bytes;


public class PipeManagerImpl {

    private String qName;

    private Map<PipeDescriptorImpl, AtomicLong> pipeCountCurrent = new HashMap<PipeDescriptorImpl, AtomicLong>();
    private final Map<PipeDescriptorImpl, Object> pipeMonitorObjMap = new HashMap<PipeDescriptorImpl, Object>();
    private Map<PipeDescriptorImpl, AtomicBoolean> pipeUseLockMap = new HashMap<PipeDescriptorImpl, AtomicBoolean>();
    private Map<String, PipeDescriptorImpl> pipeDescLookupMapByPipeNum = new HashMap<String, PipeDescriptorImpl>();
    private Map<Bytes, PipeDescriptorImpl> pipeDescLookupMapByRowKey = new HashMap<Bytes, PipeDescriptorImpl>();
    private List<Bytes> popAllKeyList = new LinkedList<Bytes>();

    public PipeManagerImpl(String qName) {
        this.qName = qName;
    }

    public void addPipe(long pipeNum) {
        addPipe(String.valueOf(pipeNum));
    }

    public void addPipe(String pipeId) {
        Bytes rowKey = Bytes.fromUTF8(QueueRepositoryImpl.formatKey(qName, pipeId));
        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(pipeId, rowKey);

        pipeCountCurrent.put(pipeDesc, new AtomicLong());
        pipeMonitorObjMap.put(pipeDesc, new Object());
        pipeUseLockMap.put(pipeDesc, new AtomicBoolean());

        pipeDescLookupMapByPipeNum.put(pipeId, pipeDesc);
        pipeDescLookupMapByRowKey.put(rowKey, pipeDesc);
        popAllKeyList.add(rowKey);

    }

    public String[] getPipeCounts() {
        String[] counts = new String[pipeCountCurrent.size()];
        int i = 0;
        for (Entry<PipeDescriptorImpl, AtomicLong> entry : pipeCountCurrent.entrySet()) {
            counts[i++] = entry.getKey().getPipeId() + " = " + entry.getValue().get();
        }
        return counts;
    }

    public PipeDescriptorImpl getPipeDescriptor(long pipeNum) {
        return getPipeDescriptor(String.valueOf(pipeNum));
    }

    public PipeDescriptorImpl getPipeDescriptor(String pipeId) {
        return pipeDescLookupMapByPipeNum.get(pipeId);
    }

    public PipeDescriptorImpl getPipeDescriptor(Bytes rowKey) {
        return pipeDescLookupMapByRowKey.get(rowKey);
    }

    public Object getPipeMonitor(PipeDescriptorImpl pipeDesc) {
        return pipeMonitorObjMap.get(pipeDesc);
    }

    public void incPushCount(PipeDescriptorImpl pipeDesc) {
        pipeCountCurrent.get(pipeDesc).incrementAndGet();
    }

    public boolean lockPopPipe(PipeDescriptorImpl pipeDesc) {
        AtomicBoolean tmpBoo = pipeUseLockMap.get(pipeDesc);
        return null != tmpBoo && tmpBoo.compareAndSet(false, true);
    }

    public void releasePopPipe(PipeDescriptorImpl pipeDesc) {
        AtomicBoolean tmpBoo = pipeUseLockMap.get(pipeDesc);
        if (null != tmpBoo) {
            tmpBoo.set(false);
        }
    }

    public void removePipe(PipeDescriptorImpl pipeDesc) {
        pipeCountCurrent.remove(pipeDesc);
        pipeMonitorObjMap.remove(pipeDesc);
        pipeUseLockMap.remove(pipeDesc);

        Bytes rowKey = Bytes.fromUTF8(QueueRepositoryImpl.formatKey(qName, pipeDesc.getPipeId()));

        pipeDescLookupMapByPipeNum.remove(pipeDesc.getPipeId());
        pipeDescLookupMapByRowKey.remove(rowKey);

        popAllKeyList.remove(rowKey);

    }

    public void shutdown() {
        // do nothing
    }

    public void truncate() {
        // private Map<PipeDescriptorImpl, AtomicLong> pipeCountCurrent = new
        // HashMap<PipeDescriptorImpl, AtomicLong>();
        // private final Map<PipeDescriptorImpl, Object> pipeMonitorObjMap = new
        // HashMap<PipeDescriptorImpl, Object>();
        // private Map<PipeDescriptorImpl, AtomicBoolean> pipeUseLockMap = new
        // HashMap<PipeDescriptorImpl, AtomicBoolean>();
        // private Map<String, PipeDescriptorImpl> pipeDescLookupMapByPipeNum = new
        // HashMap<String, PipeDescriptorImpl>();
        // private Map<Bytes, PipeDescriptorImpl> pipeDescLookupMapByRowKey = new
        // HashMap<Bytes, PipeDescriptorImpl>();
        // private List<Bytes> popAllKeyList = new LinkedList<Bytes>();
    }

    public List<Bytes> getPopAllKeyList() {
        return popAllKeyList;
    }

}
