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
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;

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
    public static final String QUEUE_KEYSPACE_NAME = "Queues";
    public static final String SYSTEM_KEYSPACE_NAME = "system";
    public static final String SYSTEM_COL_FAM = "QueueSystem";
    public static final String WAITING_COL_FAM = "WaitingQueues";
    public static final String DELIVERED_COL_FAM = "DeliveredQueues";
    public static final String STRATEGY_CLASS_NAME = "org.apache.cassandra.locator.SimpleStrategy";

    private final PelopsPool systemPool;
    private final int replicationFactor;
    private final ConsistencyLevel consistencyLevel;

    private PelopsPool queuePool;

    public QueueRepository(PelopsPool systemPool, int replicationFactor, ConsistencyLevel consistencyLevel) {
        this.systemPool = systemPool;
        this.replicationFactor = replicationFactor;
        this.consistencyLevel = consistencyLevel;
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
        Mutator m = Pelops.createMutator(queuePool.getPoolName());
        Column col = m.newColumn(name, Bytes.fromInt(width));
        m.writeColumn(SYSTEM_COL_FAM, name, col);
        m.execute(consistencyLevel);
    }

    /**
     * Truncate the queue by removing all pipes and their associated queue
     * messages.
     * 
     * @param cq
     * @throws Exception
     */
    public void truncateQueue(CassQueue cq) throws Exception {
        RowDeletor d = Pelops.createRowDeletor(queuePool.getPoolName());
        for (int i = 0; i < cq.getNumPipes(); i++) {
            String rowKey = formatKey(cq.getName(), i);
            d.deleteRow(WAITING_COL_FAM, rowKey, consistencyLevel);
            d.deleteRow(DELIVERED_COL_FAM, rowKey, consistencyLevel);
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
        Selector s = Pelops.createSelector(queuePool.getPoolName());
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, maxColumns);
        return s.getColumnsFromRow(WAITING_COL_FAM, formatKey(name, index), pred, consistencyLevel);
    }

    public List<Column> getDeliveredMessages(String name, int index, int maxColumns) throws Exception {
        Selector s = Pelops.createSelector(queuePool.getPoolName());
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, maxColumns);
        return s.getColumnsFromRow(DELIVERED_COL_FAM, formatKey(name, index), pred, consistencyLevel);
    }

    public void removeFromDelivered(Bytes key, Bytes colName) throws Exception {
        Mutator m = Pelops.createMutator(queuePool.getPoolName());
        m.deleteColumn(DELIVERED_COL_FAM, new String(key.getBytes()), colName);
        m.execute(consistencyLevel);
    }

    public void moveFromWaitingToDelivered(Bytes key, Bytes colName, Bytes colValue) throws Exception {
        Mutator m = Pelops.createMutator(queuePool.getPoolName());
        Column col = m.newColumn(colName, colValue);
        m.writeColumn(DELIVERED_COL_FAM, key, col);
        m.deleteColumn(WAITING_COL_FAM, new String(key.getBytes()), colName);
        m.execute(consistencyLevel);
    }

    public void moveFromDeliveredToWaiting(Bytes key, Bytes colName, Bytes colValue) throws Exception {

        // possibly inside ZK lock

        Mutator m = Pelops.createMutator(queuePool.getPoolName());
        Column col = m.newColumn(colName, colValue);
        m.writeColumn(WAITING_COL_FAM, key, col);
        m.deleteColumn(DELIVERED_COL_FAM, new String(key.getBytes()), colName);
        m.execute(consistencyLevel);

        // release ZK lock

    }

    /**
     * Retrieve the oldest message from all pipes.
     * 
     * @param queueKeyList
     * @return
     * @throws Exception
     */
    public Map<Bytes, List<Column>> getOldestFromAllPipes(List<Bytes> queueKeyList) throws Exception {
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, 1);
        Selector s = Pelops.createSelector(queuePool.getPoolName());
        Map<Bytes, List<Column>> colList = s.getColumnsFromRows(WAITING_COL_FAM, queueKeyList, pred, consistencyLevel);
        return colList;
    }

    /**
     * Insert a new column into the queue on the given pipe.
     * 
     * @param colFam
     * @param qName
     * @param pipeNum
     * @param colName
     * @param value
     * @throws Exception
     */
    public void insert(String colFam, String qName, int pipeNum, Bytes colName, Bytes value) throws Exception {
        Mutator m = Pelops.createMutator(queuePool.getPoolName());
        Column col = m.newColumn(colName, value);

        String rowKey = formatKey(qName, pipeNum);
        m.writeColumn(colFam, rowKey, col);
        m.execute(consistencyLevel);
    }

    private void dropKeyspace() throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(systemPool.getCluster());
        ksMgr.dropKeyspace(QUEUE_KEYSPACE_NAME);
    }

    private boolean isKeyspaceCreated() throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(systemPool.getCluster());
        Set<String> ksNameSet = ksMgr.getKeyspaceNames();
        return null != ksNameSet && ksNameSet.contains(QUEUE_KEYSPACE_NAME);
    }

    private void createKeyspace() throws Exception {
        ArrayList<CfDef> cfDefList = new ArrayList<CfDef>(2);
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, SYSTEM_COL_FAM).setComparator_type("BytesType")
                .setKey_cache_size(0).setRow_cache_size(1000));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, WAITING_COL_FAM).setComparator_type("TimeUUIDType")
                .setKey_cache_size(0));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, DELIVERED_COL_FAM).setComparator_type("TimeUUIDType")
                .setKey_cache_size(0));

        KsDef ksDef = new KsDef(QUEUE_KEYSPACE_NAME, STRATEGY_CLASS_NAME, replicationFactor, cfDefList);
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(systemPool.getCluster());
        ksMgr.addKeyspace(ksDef);
    }

    public static String formatKey(String name, int index) {
        return name + "_" + String.format("%02d", index);
    }

    public PelopsPool getQueuePool() {
        return queuePool;
    }

    public void setQueuePool(PelopsPool queuePool) {
        this.queuePool = queuePool;
    }

}
