package com.real.cassandra.queue.raw;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.wyki.cassandra.pelops.Bytes;
import org.wyki.cassandra.pelops.Management;
import org.wyki.cassandra.pelops.Mutator;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.Selector;

import com.real.cassandra.queue.CassQueue;

public class QueueRepository {
    public static final String KEYSPACE_NAME = "Queues";
    public static final String SYSTEM_COL_FAM = "QueueSystem";
    public static final String WAITING_COL_FAM = "WaitingQueues";
    public static final String DELIVERED_COL_FAM = "DeliveredQueues";
    public static final String STRATEGY_CLASS_NAME = "org.apache.cassandra.locator.RackUnawareStrategy";

    private final String poolName;
    private final ConsistencyLevel consistencyLevel;

    public QueueRepository(String poolName, ConsistencyLevel consistencyLevel) {
        this.poolName = poolName;
        this.consistencyLevel = consistencyLevel;
    }

    private void dropKeyspace() {
        Management mgmt = Pelops.createManagement(poolName);
        try {
            mgmt.dropKeyspace(KEYSPACE_NAME);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            mgmt.release();
        }

    }

    private boolean isKeyspaceCreated() throws Exception {
        Management mgmt = Pelops.createManagement(poolName);
        Set<String> ksNameSet = null;
        try {
            ksNameSet = mgmt.getKeyspaceNames();
        }
        finally {
            mgmt.release();
        }

        return null != ksNameSet && ksNameSet.contains(KEYSPACE_NAME);
    }

    private void createKeyspace() throws Exception {
        ArrayList<CfDef> cfDefList = new ArrayList<CfDef>(2);
        cfDefList.add(new CfDef(KEYSPACE_NAME, SYSTEM_COL_FAM).setComparator_type("BytesType"));
        cfDefList.add(new CfDef(KEYSPACE_NAME, WAITING_COL_FAM).setComparator_type("TimeUUIDType"));
        cfDefList.add(new CfDef(KEYSPACE_NAME, DELIVERED_COL_FAM).setComparator_type("TimeUUIDType"));

        KsDef ksDef = new KsDef(KEYSPACE_NAME, STRATEGY_CLASS_NAME, 1, cfDefList);
        Management mgmt = Pelops.createManagement(poolName);
        try {
            mgmt.addKeyspace(ksDef);
        }
        finally {
            mgmt.release();
        }
    }

    public CassQueue createQueue(String name, int width) throws Exception {
        Mutator m = Pelops.createMutator(poolName, KEYSPACE_NAME);
        Column col = m.newColumn(name, Bytes.fromInt(width));
        m.writeColumn(name, SYSTEM_COL_FAM, col);
        m.execute(consistencyLevel);
        return new CassQueue(poolName, consistencyLevel, name, width);
    }

    public void initCassandra(boolean forceRecreate) throws Exception {
        if (isKeyspaceCreated()) {
            if (!forceRecreate) {
                return;
            }
            else {
                dropKeyspace();
            }
        }

        createKeyspace();
    }

    public List<Column> getWaitingEvents(String name, int index, int maxColumns) throws Exception {
        Selector s = Pelops.createSelector(poolName, KEYSPACE_NAME);
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, maxColumns);
        return s.getColumnsFromRow(formatKey(name, index), WAITING_COL_FAM, pred, consistencyLevel);
    }

    public List<Column> getDeliveredEvents(String name, int index, int maxColumns) throws Exception {
        Selector s = Pelops.createSelector(poolName, KEYSPACE_NAME);
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, maxColumns);
        return s.getColumnsFromRow(formatKey(name, index), DELIVERED_COL_FAM, pred, consistencyLevel);
    }

    public static String formatKey(String name, int index) {
        return name + "_" + String.format("%02d", index);
    }

}
