package com.real.cassandra.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.wyki.cassandra.pelops.Bytes;
import org.wyki.cassandra.pelops.Mutator;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.Selector;
import org.wyki.cassandra.pelops.UuidHelper;

import com.real.cassandra.queue.raw.QueueRepository;

public class CassQueue {
    // private static final Bytes EMPTY_STRING_BYTES = Bytes.fromUTF8("");

    private String poolName;
    private ConsistencyLevel consistencyLevel;
    private String name;
    private int width;
    private int index = 0;
    private Object indexMonitor = new Object();
    private List<Bytes> queueKeyList;

    public CassQueue(String poolName, ConsistencyLevel consistencyLevel, String name, int width) {
        this.poolName = poolName;
        this.consistencyLevel = consistencyLevel;

        this.name = name;
        this.width = width;

        queueKeyList = new ArrayList<Bytes>(width);
        for (int i = 0; i < width; i++) {
            queueKeyList.add(Bytes.fromUTF8(QueueRepository.formatKey(name, i)));
        }
    }

    public String getName() {
        return name;
    }

    public int getWidth() {
        return width;
    }

    public void push(String value) throws Exception {
        Mutator m = Pelops.createMutator(poolName, QueueRepository.KEYSPACE_NAME);
        UUID timeUuid = UuidHelper.newTimeUuid();
        Column col = m.newColumn(Bytes.fromUuid(timeUuid), Bytes.fromUTF8(value));

        String rowKey = QueueRepository.formatKey(name, getIndexAndInc());
        m.writeColumn(rowKey, QueueRepository.WAITING_COL_FAM, col);
        m.execute(consistencyLevel);
    }

    public Event pop() throws Exception {

        // enter ZK lock region

        // get the oldest from each queue row
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, 1);
        Selector s = Pelops.createSelector(poolName, QueueRepository.KEYSPACE_NAME);
        Map<Bytes, List<Column>> colList =
                s.getColumnsFromRows(queueKeyList, QueueRepository.WAITING_COL_FAM, pred, consistencyLevel);

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

        moveFromWaitingToDelivered(rowKey, Bytes.fromUuid(oldestColName), Bytes.fromBytes(oldestColValue));

        // release ZK lock region

        return new Event(new String(rowKey.getBytes()), oldestColName, new String(oldestColValue));
    }

    public void commit(Event evt) throws Exception {
        removeFromDelivered(Bytes.fromUTF8(evt.getKey()), Bytes.fromUuid(evt.getMsgId()));
    }

    public void rollback(Event evt) throws Exception {
        moveFromDeliveredToWaiting(Bytes.fromUTF8(evt.getKey()), Bytes.fromUuid(evt.getMsgId()),
                Bytes.fromUTF8(evt.getValue()));
    }

    private void removeFromDelivered(Bytes key, Bytes colName) throws Exception {
        Mutator m = Pelops.createMutator(poolName, QueueRepository.KEYSPACE_NAME);
        m.deleteColumn(new String(key.getBytes()), QueueRepository.DELIVERED_COL_FAM, colName);
        m.execute(consistencyLevel);
    }

    private void moveFromWaitingToDelivered(Bytes key, Bytes colName, Bytes colValue) throws Exception {
        Mutator m = Pelops.createMutator(poolName, QueueRepository.KEYSPACE_NAME);
        Column col = m.newColumn(colName, colValue);
        m.writeColumn(key, QueueRepository.DELIVERED_COL_FAM, col);
        m.deleteColumn(new String(key.getBytes()), QueueRepository.WAITING_COL_FAM, colName);
        m.execute(consistencyLevel);
    }

    private void moveFromDeliveredToWaiting(Bytes key, Bytes colName, Bytes colValue) throws Exception {

        // possibly inside ZK lock

        Mutator m = Pelops.createMutator(poolName, QueueRepository.KEYSPACE_NAME);
        Column col = m.newColumn(colName, colValue);
        m.writeColumn(key, QueueRepository.WAITING_COL_FAM, col);
        m.deleteColumn(new String(key.getBytes()), QueueRepository.DELIVERED_COL_FAM, colName);
        m.execute(consistencyLevel);

        // release ZK lock

    }

    private int getIndexAndInc() {
        synchronized (indexMonitor) {
            int ret = index;
            index = (index + 1) % width;
            return ret;
        }
    }
}
