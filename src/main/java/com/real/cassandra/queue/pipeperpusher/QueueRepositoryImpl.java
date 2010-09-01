package com.real.cassandra.queue.pipeperpusher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.ColumnFamilyManager;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.repository.PelopsPool;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;

/**
 * Responsible for the raw I/O for Cassandra queues. Uses Pelops library for
 * client communication to Cassandra server.
 * 
 * <p/>
 * Requires Cassandra 0.7 or better.
 * 
 * @author Todd Burruss
 */
public class QueueRepositoryImpl extends QueueRepositoryAbstractImpl {
    private static Logger logger = LoggerFactory.getLogger(QueueRepositoryImpl.class);

    public static final String QUEUE_KEYSPACE_NAME = "Queues";

    public static final String QUEUE_DESCRIPTORS_COLFAM = "QueueDescriptors";
    public static final Bytes QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE = Bytes.fromUTF8("maxPushTimeOfPipe");
    public static final Bytes QDESC_COLNAME_MAX_PUSHES_PER_PIPE = Bytes.fromUTF8("maxPushesPerPipe");
    public static final Bytes QDESC_COLNAME_MAX_POP_WIDTH = Bytes.fromUTF8("maxPopWidth");

    public static final String ACTIVE_PIPES_COLFAM = "ActivePipes";

    public static final String PUSH_STATS_COLFAM = "PushStats";
    public static final String WAITING_COLFAM_SUFFIX = "_Waiting";
    public static final String DELIVERED_COLFAM_SUFFIX = "_Delivered";

    private static final int MAX_QUEUE_DESCRIPTOR_COLUMNS = 100;

    private QueueDescriptorFactory qDescFactory = new QueueDescriptorFactory();
    private PipeDescriptorFactory pipeDescFactory = new PipeDescriptorFactory();
    private CassQMsgFactory qMsgFactory = new CassQMsgFactory();

    public QueueRepositoryImpl(PelopsPool systemPool, PelopsPool queuePool, int replicationFactor,
            ConsistencyLevel consistencyLevel) {
        super(systemPool, queuePool, replicationFactor, consistencyLevel);
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

        // give the cluster a chance to propagate keyspaces created in previous
        Thread.sleep(2000);
    }

    /**
     * Create required column families for this new queue and initialize
     * descriptor.
     * 
     * @param cq
     * @throws Exception
     */
    public QueueDescriptor createQueueIfDoesntExist(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe,
            int maxPopWidth) throws Exception {
        ColumnFamilyManager colFamMgr =
                Pelops.createColumnFamilyManager(getSystemPool().getCluster(), QUEUE_KEYSPACE_NAME);

        CfDef colFamDef =
                new CfDef(QUEUE_KEYSPACE_NAME, formatWaitingColFamName(qName)).setComparator_type("TimeUUIDType")
                        .setKey_cache_size(0).setRow_cache_size(1000);
        try {
            colFamMgr.addColumnFamily(colFamDef);
        }
        catch (Exception e) {
            logger.info("Column family already exists, continuing : " + colFamDef.getName());
        }

        colFamDef =
                new CfDef(QUEUE_KEYSPACE_NAME, formatDeliveredColFamName(qName)).setComparator_type("TimeUUIDType")
                        .setKey_cache_size(0);
        try {
            colFamMgr.addColumnFamily(colFamDef);
        }
        catch (Exception e) {
            logger.info("Column family already exists, continuing : " + colFamDef.getName());
        }

        QueueDescriptor qDesc = getQueueDescriptor(qName);
        if (null != qDesc) {
            if (qDesc.getMaxPushesPerPipe() != maxPushesPerPipe || qDesc.getMaxPushTimeOfPipe() != maxPushTimeOfPipe
                    || qDesc.getMaxPopWidth() != maxPopWidth) {
                throw new IllegalArgumentException(
                        "Queue Descriptor already exists and you passed in maxPushesPerPipe and/or maxPushTimeOfPipe that does not match what is already there");
            }
        }
        else {
            qDesc = createQueueDescriptor(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth);
        }

        return qDesc;
    }

    public QueueDescriptor getQueueDescriptor(String qName) throws Exception {
        Selector s = Pelops.createSelector(getQueuePool().getPoolName());

        // make sure that enough columns are retrieved
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, MAX_QUEUE_DESCRIPTOR_COLUMNS);
        List<Column> colList = s.getColumnsFromRow(QUEUE_DESCRIPTORS_COLFAM, qName, pred, getConsistencyLevel());
        if (null != colList && !colList.isEmpty()) {
            return qDescFactory.createInstance(qName, colList);
        }
        else {
            return null;
        }

    }

    public QueueDescriptor createQueueDescriptor(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe,
            int maxPopWidth) throws Exception {
        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());

        Column col = m.newColumn(QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE, Bytes.fromLong(maxPushTimeOfPipe));
        m.writeColumn(QUEUE_DESCRIPTORS_COLFAM, qName, col);

        col = m.newColumn(QDESC_COLNAME_MAX_PUSHES_PER_PIPE, Bytes.fromInt(maxPushesPerPipe));
        m.writeColumn(QUEUE_DESCRIPTORS_COLFAM, qName, col);

        col = m.newColumn(QDESC_COLNAME_MAX_POP_WIDTH, Bytes.fromInt(maxPopWidth));
        m.writeColumn(QUEUE_DESCRIPTORS_COLFAM, qName, col);

        m.execute(getConsistencyLevel());

        return qDescFactory.createInstance(qName, maxPushTimeOfPipe, maxPushesPerPipe);
    }

    /**
     * Retrieve a specific message from a specific pipe in the queue. Not very
     * useful for production but helps during testing.
     * 
     * @param qName
     * @param pipeId
     * @param msgId
     * @return
     * @throws Exception
     */
    public CassQMsg getMsg(String qName, UUID pipeId, UUID msgId) throws Exception {
        Selector s = Pelops.createSelector(getQueuePool().getPoolName());
        String colFamWaiting = formatWaitingColFamName(qName);
        Bytes rowKey = Bytes.fromUuid(pipeId);
        Bytes colName = Bytes.fromUuid(msgId);
        Column col;
        try {
            col = s.getColumnFromRow(colFamWaiting, rowKey, colName, getConsistencyLevel());
        }
        catch (NotFoundException e) {
            return null;
        }

        PipeDescriptorImpl pipeDesc = getPipeDescriptor(qName, pipeId);
        CassQMsg qMsg = qMsgFactory.createInstance(pipeDesc, msgId, new String(col.getValue()));
        return qMsg;
    }

    public void createPipeDescriptor(String qName, UUID pipeId, String pipeStatus) throws Exception {
        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());

        Column col = m.newColumn(Bytes.fromUuid(pipeId), pipeStatus);
        m.writeColumn(ACTIVE_PIPES_COLFAM, qName, col);

        col = m.newColumn(Bytes.fromUuid(pipeId), Bytes.fromInt(0));
        m.writeColumn(PUSH_STATS_COLFAM, qName, col);

        m.execute(getConsistencyLevel());
    }

    public void setPipeDescriptorStatus(String qName, UUID pipeId, String status) throws Exception {
        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        Column col = m.newColumn(Bytes.fromUuid(pipeId), status);
        m.writeColumn(ACTIVE_PIPES_COLFAM, qName, col);
        m.execute(getConsistencyLevel());
    }

    /**
     * Get specific pipe descriptor from queue.
     * 
     * @param qName
     * @param pipeId
     * @return
     * @throws Exception
     */
    public PipeDescriptorImpl getPipeDescriptor(String qName, UUID pipeId) throws Exception {
        Selector s = Pelops.createSelector(getQueuePool().getPoolName());
        Bytes colName = Bytes.fromUuid(pipeId);
        try {
            Column colActive = s.getColumnFromRow(ACTIVE_PIPES_COLFAM, qName, colName, getConsistencyLevel());
            Column colStats = s.getColumnFromRow(PUSH_STATS_COLFAM, qName, colName, getConsistencyLevel());
            return pipeDescFactory.createInstance(qName, colActive, colStats);
        }
        catch (NotFoundException e) {
            return pipeDescFactory.createInstance(qName, pipeId, PipeDescriptorImpl.STATUS_PUSH_ACTIVE);
        }
    }

    public void insert(String qName, PipeDescriptorImpl pipeDesc, UUID msgId, String msgData) throws Exception {
        Bytes colName = Bytes.fromUuid(msgId);
        Bytes value = Bytes.fromUTF8(msgData);

        String colFamWaiting = formatWaitingColFamName(qName);

        // insert new msg
        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        Column col = m.newColumn(colName, value);
        m.writeColumn(colFamWaiting, Bytes.fromUuid(pipeDesc.getPipeId()), col);

        // update msg count for this pipe
        col = m.newColumn(Bytes.fromUuid(pipeDesc.getPipeId()), Bytes.fromInt(pipeDesc.getMsgCount()));
        m.writeColumn(PUSH_STATS_COLFAM, qName, col);

        m.execute(getConsistencyLevel());
    }

    public static String formatWaitingColFamName(String qName) {
        return qName + WAITING_COLFAM_SUFFIX;
    }

    public static String formatDeliveredColFamName(String qName) {
        return qName + DELIVERED_COLFAM_SUFFIX;
    }

    private void dropKeyspace() throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(getSystemPool().getCluster());
        ksMgr.dropKeyspace(QUEUE_KEYSPACE_NAME);
    }

    private boolean isKeyspaceCreated() throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(getSystemPool().getCluster());
        Set<String> ksNameSet = ksMgr.getKeyspaceNames();
        return null != ksNameSet && ksNameSet.contains(QUEUE_KEYSPACE_NAME);
    }

    private void createKeyspace() throws Exception {
        ArrayList<CfDef> cfDefList = new ArrayList<CfDef>(2);
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, QUEUE_DESCRIPTORS_COLFAM).setComparator_type("BytesType")
                .setKey_cache_size(0).setRow_cache_size(1000));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, ACTIVE_PIPES_COLFAM).setComparator_type("TimeUUIDType")
                .setKey_cache_size(0).setRow_cache_size(1000));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, PUSH_STATS_COLFAM).setComparator_type("TimeUUIDType")
                .setKey_cache_size(0));

        KsDef ksDef = new KsDef(QUEUE_KEYSPACE_NAME, STRATEGY_CLASS_NAME, getReplicationFactor(), cfDefList);
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(getSystemPool().getCluster());
        ksMgr.addKeyspace(ksDef);
    }

    public CassQMsg getOldestMsgFromDeliveredPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        return getOldestMsgFromPipe(formatDeliveredColFamName(pipeDesc.getQName()), pipeDesc);
    }

    public CassQMsg getOldestMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        return getOldestMsgFromPipe(formatWaitingColFamName(pipeDesc.getQName()), pipeDesc);
    }

    private CassQMsg getOldestMsgFromPipe(String colFamName, PipeDescriptorImpl pipeDesc) throws Exception {
        List<CassQMsg> msgList = getMessagesFromPipe(colFamName, pipeDesc, 1);
        if (null != msgList && !msgList.isEmpty()) {
            return msgList.get(0);
        }
        else {
            return null;
        }
    }

    public List<CassQMsg> getDeliveredMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxColumns) throws Exception {
        return getMessagesFromPipe(formatDeliveredColFamName(pipeDesc.getQName()), pipeDesc, maxColumns);
    }

    public List<CassQMsg> getWaitingMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxColumns) throws Exception {
        return getMessagesFromPipe(formatWaitingColFamName(pipeDesc.getQName()), pipeDesc, maxColumns);
    }

    private List<CassQMsg> getMessagesFromPipe(String colFameName, PipeDescriptorImpl pipeDesc, int maxColumns)
            throws Exception {
        Selector s = Pelops.createSelector(getQueuePool().getPoolName());
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, maxColumns);
        List<Column> colList =
                s.getColumnsFromRow(colFameName, Bytes.fromUuid(pipeDesc.getPipeId()), pred, getConsistencyLevel());
        ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(maxColumns);
        for (Column col : colList) {
            msgList.add(qMsgFactory.createInstance(pipeDesc, col));
        }
        return msgList;
    }

    public void removeMsgFromDeliveredPipe(PipeDescriptorImpl pipeDesc, CassQMsg qMsg) throws Exception {
        removeMsgFromPipe(formatDeliveredColFamName(pipeDesc.getQName()), pipeDesc, qMsg);
    }

    public void removeMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc, CassQMsg qMsg) throws Exception {
        removeMsgFromPipe(formatWaitingColFamName(pipeDesc.getQName()), pipeDesc, qMsg);
    }

    private void removeMsgFromPipe(String colFamName, PipeDescriptorImpl pipeDesc, CassQMsg qMsg) throws Exception {
        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        m.deleteColumn(colFamName, Bytes.fromUuid(pipeDesc.getPipeId()), Bytes.fromUuid(qMsg.getMsgId()));
        m.execute(getConsistencyLevel());
    }

    public void moveMsgFromWaitingToDeliveredPipe(PipeDescriptorImpl pipeDesc, CassQMsg qMsg) throws Exception {
        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        Bytes colName = Bytes.fromUuid(qMsg.getMsgId());
        Bytes colValue = Bytes.fromUTF8(qMsg.getMsgData());
        Bytes pipeId = Bytes.fromUuid(pipeDesc.getPipeId());
        m.writeColumn(formatDeliveredColFamName(pipeDesc.getQName()), pipeId, m.newColumn(colName, colValue));
        m.deleteColumn(formatWaitingColFamName(pipeDesc.getQName()), pipeId, colName);
        m.execute(getConsistencyLevel());
    }

    public List<PipeDescriptorImpl> getOldestNonEmptyPipes(String qName, int maxNumPipeDescs) throws Exception {
        Selector s = Pelops.createSelector(getQueuePool().getPoolName());
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, maxNumPipeDescs);
        Bytes qNameAsBytes = Bytes.fromUTF8(qName);

        List<Column> statusColList =
                s.getColumnsFromRow(ACTIVE_PIPES_COLFAM, qNameAsBytes, pred, getConsistencyLevel());
        List<Column> statsColList = s.getColumnsFromRow(PUSH_STATS_COLFAM, qNameAsBytes, pred, getConsistencyLevel());

        ArrayList<PipeDescriptorImpl> pipeDescList = new ArrayList<PipeDescriptorImpl>(maxNumPipeDescs);

        if (statusColList.size() != statsColList.size()) {
            logger.error("active pipes count doesn't equal stats count - continuing using active pipes as reference");
        }

        Iterator<Column> statsIter = statsColList.iterator();
        for (Column activeCol : statusColList) {
            Column statCol = null;
            if (statsIter.hasNext()) {
                statCol = statsIter.next();
            }
            PipeDescriptorImpl pipeDesc = pipeDescFactory.createInstance(qName, activeCol, statCol);
            if (!pipeDesc.isFinishedAndEmpty()) {
                pipeDescList.add(pipeDesc);
            }
        }
        return pipeDescList;

    }
    // public List<Column> getDeliveredMessages(String name, long pipeNum, int
    // maxColumns) throws Exception {
    // Selector s = Pelops.createSelector(queuePool.getPoolName());
    // SlicePredicate pred = Selector.newColumnsPredicateAll(false, maxColumns);
    // return s.getColumnsFromRow(DELIVERED_COL_FAM, formatKey(name, pipeNum),
    // pred, consistencyLevel);
    // }

    // public boolean isQueueExists(String qName) throws Exception {
    // return null != getQueueDescriptor(qName);
    // }

    // public int getCount(String qName) throws Exception {
    // QueueDescriptor qDesc = getQueueDescriptor(qName);
    // return getCount(qName, qDesc.getPopStartPipe(), qDesc.getPushStartPipe()
    // + qDesc.getNumPipes());
    // }

    // public int getCount(String qName, long startPipe, long endPipe) throws
    // Exception {
    // int count = 0;
    // for (long pipeNum = startPipe; pipeNum <= endPipe; pipeNum++) {
    // Selector s = Pelops.createSelector(queuePool.getPoolName());
    // SlicePredicate predicate = Selector.newColumnsPredicateAll(false,
    // 1000000);
    // count += s.getColumnCount(WAITING_COL_FAM, formatKey(qName, pipeNum),
    // predicate, consistencyLevel);
    // }
    // return count;
    // }

    // /**
    // * Truncate the queue by removing all pipes and their associated queue
    // * messages.
    // *
    // * @param cq
    // * @throws Exception
    // */
    // public void truncateQueueData(CassQueueImpl cq) throws Exception {
    // // ColumnFamilyManager cfMgr =
    // // Pelops.createColumnFamilyManager(systemPool.getCluster(),
    // // QUEUE_KEYSPACE_NAME);
    // // cfMgr.truncateColumnFamily(columnFamily);
    //
    // QueueDescriptor qDesc = getQueueDescriptor(cq.getName());
    // long startPipe = qDesc.getPopStartPipe();
    // long endPipe = qDesc.getPushStartPipe() + qDesc.getNumPipes();
    //
    // RowDeletor d = Pelops.createRowDeletor(queuePool.getPoolName());
    // for (long pipeNum = startPipe; pipeNum <= endPipe; pipeNum++) {
    // String rowKey = formatKey(cq.getName(), pipeNum);
    // d.deleteRow(WAITING_COL_FAM, rowKey, consistencyLevel);
    // d.deleteRow(DELIVERED_COL_FAM, rowKey, consistencyLevel);
    // }
    // }

    // public void removeFromDelivered(PipeDescriptorImpl pipeDesc, Bytes
    // colName) throws Exception {
    // Mutator m = Pelops.createMutator(queuePool.getPoolName());
    // m.deleteColumn(DELIVERED_COL_FAM, new
    // String(pipeDesc.getRowKey().getBytes()), colName);
    // m.execute(consistencyLevel);
    // }

    // public void moveFromDeliveredToWaiting(PipeDescriptorImpl pipeDesc, Bytes
    // colName, Bytes colValue) throws Exception {
    //
    // Mutator m = Pelops.createMutator(queuePool.getPoolName());
    // Column col = m.newColumn(colName, colValue);
    // m.writeColumn(WAITING_COL_FAM, pipeDesc.getRowKey(), col);
    // m.deleteColumn(DELIVERED_COL_FAM, pipeDesc.getRowKey().toString(),
    // colName);
    // m.execute(consistencyLevel);
    //
    // }
    //
    // public Map<Bytes, List<Column>> getOldestFromAllPipes(List<Bytes>
    // queueKeyList) throws Exception {
    // SlicePredicate pred = Selector.newColumnsPredicateAll(false, 1);
    // Selector s = Pelops.createSelector(queuePool.getPoolName());
    // Map<Bytes, List<Column>> colList = s.getColumnsFromRows(WAITING_COL_FAM,
    // queueKeyList, pred, consistencyLevel);
    // return colList;
    // }
    //
    // public static String formatKey(String name, long pipeId) {
    // return formatKey(name, String.valueOf(pipeId));
    // }
    //
    // public static String formatKey(String name, String pipeId) {
    // return name + "-" + pipeId;
    // }
    //
    // public PelopsPool getQueuePool() {
    // return queuePool;
    // }
    //
    // public void setQueuePool(PelopsPool queuePool) {
    // this.queuePool = queuePool;
    // }

}
