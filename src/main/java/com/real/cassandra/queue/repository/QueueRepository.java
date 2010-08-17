package com.real.cassandra.queue.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.wyki.cassandra.pelops.Bytes;
import org.wyki.cassandra.pelops.KeyDeletor;
import org.wyki.cassandra.pelops.Management;
import org.wyki.cassandra.pelops.Mutator;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.Selector;

import com.real.cassandra.queue.CassQueue;

/**
 * Responsible for the raw I/O for Cassandra queues. Uses Pelops library for
 * client communication to Cassandra server.
 * 
 * <p/>
 * Requires Cassandra 0.7 or better.
 * 
 * @author Todd Burruss
 */
public class QueueRepository {
    public static final String KEYSPACE_NAME = "Queues";
    public static final String SYSTEM_COL_FAM = "QueueSystem";
    public static final String WAITING_COL_FAM = "WaitingQueues";
    public static final String DELIVERED_COL_FAM = "DeliveredQueues";
    public static final String STRATEGY_CLASS_NAME = "org.apache.cassandra.locator.RackUnawareStrategy";

    private final PelopsPool pool;
    private final int replicationFactor;
    private final ConsistencyLevel consistencyLevel;

    public QueueRepository(PelopsPool pool, int replicationFactor, ConsistencyLevel consistencyLevel) {
        this.pool = pool;
        this.replicationFactor = replicationFactor;
        this.consistencyLevel = consistencyLevel;
    }

    private void dropKeyspace() {
        Management mgmt = Pelops.createManagement(pool.getPoolName());
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
        Management mgmt = Pelops.createManagement(pool.getPoolName());
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

        KsDef ksDef = new KsDef(KEYSPACE_NAME, STRATEGY_CLASS_NAME, replicationFactor, cfDefList);
        Management mgmt = Pelops.createManagement(pool.getPoolName());
        try {
            mgmt.addKeyspace(ksDef);
        }
        finally {
            mgmt.release();
        }
    }

    /**
     * Create a new empty queue. If queue already exists, an exception is
     * thrown.
     * 
     * @param name
     * @param width
     * @return
     * @throws Exception
     */
    public void createQueue(String name, int width) throws Exception {
        Mutator m = Pelops.createMutator(pool.getPoolName(), KEYSPACE_NAME);
        Column col = m.newColumn(name, Bytes.fromInt(width));
        m.writeColumn(name, SYSTEM_COL_FAM, col);
        m.execute(consistencyLevel);
    }

    public void truncateQueue(CassQueue cq) throws Exception {
        KeyDeletor d = Pelops.createKeyDeletor(pool.getPoolName(), KEYSPACE_NAME);
        for (int i = 0; i < cq.getNumPipes(); i++) {
            String rowKey = formatKey(cq.getName(), i);
            d.deleteRow(rowKey, WAITING_COL_FAM, consistencyLevel);
            d.deleteRow(rowKey, DELIVERED_COL_FAM, consistencyLevel);
        }
    }

    /**
     * Perform default initialization of the repository.
     * 
     * @throws Exception
     */
    public void init() throws Exception {
        initCassandra(false);
    }

    /**
     * Initialize cassandra server for use with queues. If keyspace already
     * exists, nothing is done.
     * 
     * @param forceRecreate
     *            if true will drop the keyspace and recreate it.
     * @throws Exception
     */
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

    public List<Column> getWaitingMessages(String name, int index, int maxColumns) throws Exception {
        Selector s = Pelops.createSelector(pool.getPoolName(), KEYSPACE_NAME);
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, maxColumns);
        return s.getColumnsFromRow(formatKey(name, index), WAITING_COL_FAM, pred, consistencyLevel);
    }

    public List<Column> getDeliveredMessages(String name, int index, int maxColumns) throws Exception {
        Selector s = Pelops.createSelector(pool.getPoolName(), KEYSPACE_NAME);
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, maxColumns);
        return s.getColumnsFromRow(formatKey(name, index), DELIVERED_COL_FAM, pred, consistencyLevel);
    }

    public void removeFromDelivered(Bytes key, Bytes colName) throws Exception {
        Mutator m = Pelops.createMutator(pool.getPoolName(), KEYSPACE_NAME);
        m.deleteColumn(new String(key.getBytes()), DELIVERED_COL_FAM, colName);
        m.execute(consistencyLevel);
    }

    public void moveFromWaitingToDelivered(Bytes key, Bytes colName, Bytes colValue) throws Exception {
        Mutator m = Pelops.createMutator(pool.getPoolName(), KEYSPACE_NAME);
        Column col = m.newColumn(colName, colValue);
        m.writeColumn(key, DELIVERED_COL_FAM, col);
        m.deleteColumn(new String(key.getBytes()), WAITING_COL_FAM, colName);
        m.execute(consistencyLevel);
    }

    public void moveFromDeliveredToWaiting(Bytes key, Bytes colName, Bytes colValue) throws Exception {

        // possibly inside ZK lock

        Mutator m = Pelops.createMutator(pool.getPoolName(), KEYSPACE_NAME);
        Column col = m.newColumn(colName, colValue);
        m.writeColumn(key, WAITING_COL_FAM, col);
        m.deleteColumn(new String(key.getBytes()), DELIVERED_COL_FAM, colName);
        m.execute(consistencyLevel);

        // release ZK lock

    }

    public Map<Bytes, List<Column>> getOldestFromAllPipes(List<Bytes> queueKeyList) throws Exception {
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, 1);
        Selector s = Pelops.createSelector(pool.getPoolName(), KEYSPACE_NAME);
        Map<Bytes, List<Column>> colList = s.getColumnsFromRows(queueKeyList, WAITING_COL_FAM, pred, consistencyLevel);
        return colList;
    }

    public void insert(String colFam, String qName, int pipeNum, Bytes colName, Bytes value) throws Exception {
        Mutator m = Pelops.createMutator(pool.getPoolName(), KEYSPACE_NAME);
        Column col = m.newColumn(colName, value);

        String rowKey = formatKey(qName, pipeNum);
        m.writeColumn(rowKey, colFam, col);
        m.execute(consistencyLevel);
    }

    public static String formatKey(String name, int index) {
        return name + "_" + String.format("%02d", index);
    }

}
