package com.real.cassandra.queue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.scale7.cassandra.pelops.Bytes;

import com.real.cassandra.queue.repository.QueueRepository;

public class PipeManager {

    private String qName;

    private Map<Long, AtomicLong> pipeCountCurrent = new HashMap<Long, AtomicLong>();
    private final Map<Long, Object> pipeMonitorObjMap = new HashMap<Long, Object>();
    private Map<Long, AtomicBoolean> pipeUseLockMap = new HashMap<Long, AtomicBoolean>();
    private Map<Long, PipeDescriptor> pipeDescLookupMapByPipeNum = new HashMap<Long, PipeDescriptor>();
    private Map<Bytes, PipeDescriptor> pipeDescLookupMapByRowKey = new HashMap<Bytes, PipeDescriptor>();
    private List<Bytes> popAllKeyList = new LinkedList<Bytes>();

    public PipeManager(String qName) {
        this.qName = qName;
    }

    public void addPipe(Long pipeNum) {
        pipeCountCurrent.put(pipeNum, new AtomicLong());
        pipeMonitorObjMap.put(pipeNum, new Object());
        pipeUseLockMap.put(pipeNum, new AtomicBoolean());

        Bytes rowKey = Bytes.fromUTF8(QueueRepository.formatKey(qName, pipeNum));

        PipeDescriptor pipeDesc = new PipeDescriptor(pipeNum, rowKey);
        pipeDescLookupMapByPipeNum.put(pipeNum, pipeDesc);
        pipeDescLookupMapByRowKey.put(rowKey, pipeDesc);
        popAllKeyList.add(rowKey);

    }

    public String[] getPipeCounts() {
        String[] counts = new String[pipeCountCurrent.size()];
        int i = 0;
        for (Entry<Long, AtomicLong> entry : pipeCountCurrent.entrySet()) {
            counts[i++] = entry.getKey() + " = " + entry.getValue().get();
        }
        return counts;
    }

    public PipeDescriptor getPipeDescriptor(Long pipeNum) {
        return pipeDescLookupMapByPipeNum.get(pipeNum);
    }

    public PipeDescriptor getPipeDescriptor(Bytes rowKey) {
        return pipeDescLookupMapByRowKey.get(rowKey);
    }

    public Object getPipeMonitor(PipeDescriptor pipeDesc) {
        return pipeMonitorObjMap.get(pipeDesc.getPipeNum());
    }

    public void incPushCount(Long pipeNum) {
        pipeCountCurrent.get(pipeNum).incrementAndGet();
    }

    public boolean lockPopPipe(PipeDescriptor pipeDesc) {
        AtomicBoolean tmpBoo = pipeUseLockMap.get(pipeDesc.getPipeNum());
        return null != tmpBoo && tmpBoo.compareAndSet(false, true);
    }

    public void releasePopPipe(Long pipeNum) {
        AtomicBoolean tmpBoo = pipeUseLockMap.get(pipeNum);
        if (null != tmpBoo) {
            tmpBoo.set(false);
        }
    }

    public void removePipe(Long pipeNum) {
        pipeCountCurrent.remove(pipeNum);
        pipeMonitorObjMap.remove(pipeNum);
        pipeUseLockMap.remove(pipeNum);

        Bytes rowKey = Bytes.fromUTF8(QueueRepository.formatKey(qName, pipeNum));

        pipeDescLookupMapByPipeNum.remove(rowKey);
        popAllKeyList.remove(rowKey);

    }

    public List<Bytes> getPopAllKeyList() {
        return popAllKeyList;
    }

}
