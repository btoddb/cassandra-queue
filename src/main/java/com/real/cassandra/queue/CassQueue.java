package com.real.cassandra.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.cassandra.thrift.Column;
import org.wyki.cassandra.pelops.Bytes;
import org.wyki.cassandra.pelops.UuidHelper;

public class CassQueue {
    // private static final Bytes EMPTY_STRING_BYTES = Bytes.fromUTF8("");

    private QueueRepository queueRepository;
    private String name;
    private int numPipes;
    private int curPipe = 0;
    private Object indexMonitor = new Object();
    private List<Bytes> queueKeyList;

    public CassQueue(QueueRepository queueRepository, String name, int numPipes) {
        this.queueRepository = queueRepository;

        this.name = name;
        this.numPipes = numPipes;

        queueKeyList = new ArrayList<Bytes>(numPipes);
        for (int i = 0; i < numPipes; i++) {
            queueKeyList.add(Bytes.fromUTF8(QueueRepository.formatKey(name, i)));
        }
    }

    public String getName() {
        return name;
    }

    public int getNumPipes() {
        return numPipes;
    }

    public void push(String value) throws Exception {
        UUID timeUuid = UuidHelper.newTimeUuid();
        queueRepository.insert(QueueRepository.WAITING_COL_FAM, getName(), getIndexAndInc(), Bytes.fromUuid(timeUuid),
                Bytes.fromUTF8(value));
    }

    public Event pop() throws Exception {

        // enter ZK lock region

        Map<Bytes, List<Column>> colList = queueRepository.getOldestFromAllPipes(queueKeyList);

        // determine which result is the oldest across the queue rows
        Bytes rowKey = null;
        UUID oldestColName = null;
        byte[] oldestColValue = null;
        for (Entry<Bytes, List<Column>> entry : colList.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }

            Column tmpCol = entry.getValue().get(0);
            UUID colName = UuidHelper.timeUuidFromBytes(tmpCol.getName());
            if (null == rowKey || -1 == colName.compareTo(oldestColName)) {
                rowKey = entry.getKey();
                oldestColName = colName;
                oldestColValue = tmpCol.getValue();
            }
        }

        // if no "oldest", then return null
        if (null == rowKey) {
            return null;
        }

        queueRepository.moveFromWaitingToDelivered(rowKey, Bytes.fromUuid(oldestColName),
                Bytes.fromBytes(oldestColValue));

        // release ZK lock region

        return new Event(new String(rowKey.getBytes()), oldestColName, new String(oldestColValue));
    }

    public void commit(Event evt) throws Exception {
        queueRepository.removeFromDelivered(Bytes.fromUTF8(evt.getKey()), Bytes.fromUuid(evt.getMsgId()));
    }

    public void rollback(Event evt) throws Exception {
        queueRepository.moveFromDeliveredToWaiting(Bytes.fromUTF8(evt.getKey()), Bytes.fromUuid(evt.getMsgId()),
                Bytes.fromUTF8(evt.getValue()));
    }

    public void truncate() throws Exception {
        // enter ZK lock region

        synchronized (indexMonitor) {
            queueRepository.truncateQueue(this);
            curPipe = 0;
        }

        // release ZK lock region
    }

    private int getIndexAndInc() {
        synchronized (indexMonitor) {
            int ret = curPipe;
            curPipe = (curPipe + 1) % numPipes;
            return ret;
        }
    }

    public List<Column> getWaitingEvents(int index, int maxNumEvents) throws Exception {
        return queueRepository.getWaitingEvents(getName(), index, maxNumEvents);
    }

    public List<Column> getDeliveredEvents(int index, int maxNumEvents) throws Exception {
        return queueRepository.getDeliveredEvents(getName(), index, maxNumEvents);
    }

}
