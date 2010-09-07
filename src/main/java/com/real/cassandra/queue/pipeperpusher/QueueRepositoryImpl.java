package com.real.cassandra.queue.pipeperpusher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    public static final Bytes QDESC_COLNAME_POP_PIPE_REFRESH_DELAY = Bytes.fromUTF8("popPipeRefreshDelay");

    public static final String PIPE_STATUS_COLFAM = "PipeStatus";

    public static final String WAITING_COLFAM_SUFFIX = "_Waiting";
    public static final String DELIVERED_COLFAM_SUFFIX = "_Delivered";
    public static final int GC_GRACE_SECS = 86400; // one day

    private static final int MAX_QUEUE_DESCRIPTOR_COLUMNS = 100;

    private QueueDescriptorFactory qDescFactory = new QueueDescriptorFactory();
    private PipeDescriptorFactory pipeDescFactory = new PipeDescriptorFactory();
    private CassQMsgFactory qMsgFactory = new CassQMsgFactory();
    private PipeStatusFactory pipeStatusFactory = new PipeStatusFactory();

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
    public QueueDescriptor createQueueIfDoesntExist(String qName, long maxPushTimePerPipe, int maxPushesPerPipe,
            int maxPopWidth, long popPipeRefreshDelay) throws Exception {
        ColumnFamilyManager colFamMgr =
                Pelops.createColumnFamilyManager(getSystemPool().getCluster(), QUEUE_KEYSPACE_NAME);

        CfDef colFamDef =
                new CfDef(QUEUE_KEYSPACE_NAME, formatWaitingColFamName(qName)).setComparator_type("TimeUUIDType")
                        .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS);
        try {
            colFamMgr.addColumnFamily(colFamDef);
        }
        catch (Exception e) {
            logger.info("exception while trying to create column family, " + colFamDef.getName()
                    + " - possibly already exists and is OK");
        }

        colFamDef =
                new CfDef(QUEUE_KEYSPACE_NAME, formatDeliveredColFamName(qName)).setComparator_type("TimeUUIDType")
                        .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS);
        try {
            colFamMgr.addColumnFamily(colFamDef);
        }
        catch (Exception e) {
            logger.info("exception while trying to create column family, " + colFamDef.getName()
                    + " - possibly already exists and is OK");
        }

        QueueDescriptor qDesc = getQueueDescriptor(qName);
        if (null != qDesc) {
            if (qDesc.getMaxPushesPerPipe() != maxPushesPerPipe || qDesc.getMaxPushTimeOfPipe() != maxPushTimePerPipe
                    || qDesc.getMaxPopWidth() != maxPopWidth) {
                throw new IllegalArgumentException(
                        "Queue Descriptor already exists and you passed in maxPushesPerPipe and/or maxPushTimeOfPipe that does not match what is already there");
            }
        }
        else {
            qDesc =
                    createQueueDescriptor(qName, maxPushTimePerPipe, maxPushesPerPipe, maxPopWidth, popPipeRefreshDelay);
        }

        return qDesc;
    }

    public void removeQueue(CassQueueImpl cq) throws Exception {
        String qName = cq.getName();

        ColumnFamilyManager colFamMgr =
                Pelops.createColumnFamilyManager(getSystemPool().getCluster(), QUEUE_KEYSPACE_NAME);
        try {
            colFamMgr.dropColumnFamily(formatWaitingColFamName(qName));
        }
        catch (Exception e) {
            logger.info("exception while trying to drop column family, " + formatWaitingColFamName(qName));
        }

        try {
            colFamMgr.dropColumnFamily(formatDeliveredColFamName(qName));
        }
        catch (Exception e) {
            logger.info("exception while trying to drop column family, " + formatDeliveredColFamName(qName));
        }

        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        m.removeRow(PIPE_STATUS_COLFAM, Bytes.fromUTF8(qName), getConsistencyLevel());
        m.execute(getConsistencyLevel());

        m = Pelops.createMutator(getQueuePool().getPoolName());
        m.removeRow(QUEUE_DESCRIPTORS_COLFAM, Bytes.fromUTF8(qName), getConsistencyLevel());
        m.execute(getConsistencyLevel());
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
            int maxPopWidth, long popPipeRefreshDelay) throws Exception {
        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());

        Column col = m.newColumn(QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE, Bytes.fromLong(maxPushTimeOfPipe));
        m.writeColumn(QUEUE_DESCRIPTORS_COLFAM, qName, col);

        col = m.newColumn(QDESC_COLNAME_MAX_PUSHES_PER_PIPE, Bytes.fromInt(maxPushesPerPipe));
        m.writeColumn(QUEUE_DESCRIPTORS_COLFAM, qName, col);

        col = m.newColumn(QDESC_COLNAME_MAX_POP_WIDTH, Bytes.fromInt(maxPopWidth));
        m.writeColumn(QUEUE_DESCRIPTORS_COLFAM, qName, col);

        col = m.newColumn(QDESC_COLNAME_POP_PIPE_REFRESH_DELAY, Bytes.fromLong(popPipeRefreshDelay));
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
        String rawStatus = pipeStatusFactory.createInstance(new PipeStatus(pipeStatus, 0));

        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        Column col = m.newColumn(Bytes.fromUuid(pipeId), Bytes.fromUTF8(rawStatus));
        m.writeColumn(PIPE_STATUS_COLFAM, qName, col);
        m.execute(getConsistencyLevel());
    }

    public void setPipeDescriptorStatus(String qName, PipeDescriptorImpl pipeDesc, String pipeStatus) throws Exception {
        String rawStatus = pipeStatusFactory.createInstance(new PipeStatus(pipeStatus, pipeDesc.getMsgCount()));

        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        Column col = m.newColumn(Bytes.fromUuid(pipeDesc.getPipeId()), rawStatus);
        m.writeColumn(PIPE_STATUS_COLFAM, qName, col);
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
            Column colStatus = s.getColumnFromRow(PIPE_STATUS_COLFAM, qName, colName, getConsistencyLevel());
            return pipeDescFactory.createInstance(qName, colStatus);
        }
        catch (NotFoundException e) {
            return pipeDescFactory.createInstance(qName, pipeId, PipeDescriptorImpl.STATUS_PUSH_ACTIVE, 0);
        }
    }

    public void insert(String qName, PipeDescriptorImpl pipeDesc, UUID msgId, String msgData) throws Exception {
        Bytes colName = Bytes.fromUuid(msgId);
        Bytes colValue = Bytes.fromUTF8(msgData);

        String colFamWaiting = formatWaitingColFamName(qName);

        // insert new msg
        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        Column col = m.newColumn(colName, colValue);
        m.writeColumn(colFamWaiting, Bytes.fromUuid(pipeDesc.getPipeId()), col);

        String rawStatus =
                pipeStatusFactory.createInstance(new PipeStatus(PipeDescriptorImpl.STATUS_PUSH_ACTIVE, pipeDesc
                        .getMsgCount()));

        // update msg count for this pipe
        col = m.newColumn(Bytes.fromUuid(pipeDesc.getPipeId()), Bytes.fromUTF8(rawStatus));
        m.writeColumn(PIPE_STATUS_COLFAM, qName, col);

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
                .setKey_cache_size(0).setRow_cache_size(1000).setGc_grace_seconds(GC_GRACE_SECS));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, PIPE_STATUS_COLFAM).setComparator_type("TimeUUIDType")
                .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS));

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

    public List<CassQMsg> getDeliveredMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxMsgs) throws Exception {
        return getMessagesFromPipe(formatDeliveredColFamName(pipeDesc.getQName()), pipeDesc, maxMsgs);
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

    public void removeMsgFromDeliveredPipe(CassQMsg qMsg) throws Exception {
        PipeDescriptorImpl pipeDesc = qMsg.getPipeDescriptor();
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

    public void moveMsgFromWaitingToDeliveredPipe(CassQMsg qMsg) throws Exception {
        PipeDescriptorImpl pipeDesc = qMsg.getPipeDescriptor();
        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        Bytes colName = Bytes.fromUuid(qMsg.getMsgId());
        Bytes colValue = Bytes.fromUTF8(qMsg.getMsgData());
        Bytes pipeId = Bytes.fromUuid(pipeDesc.getPipeId());
        m.writeColumn(formatDeliveredColFamName(pipeDesc.getQName()), pipeId, m.newColumn(colName, colValue));
        m.deleteColumn(formatWaitingColFamName(pipeDesc.getQName()), pipeId, colName);
        m.execute(getConsistencyLevel());
    }

    public List<PipeDescriptorImpl> getOldestNonEmptyPipes(final String qName, final int maxNumPipeDescs)
            throws Exception {
        final List<PipeDescriptorImpl> pipeDescList = new LinkedList<PipeDescriptorImpl>();
        // final Bytes qNameAsBytes = Bytes.fromUTF8(qName);

        ColumnIterator rawMsgColIter = new ColumnIterator();
        rawMsgColIter.doIt(getQueuePool().getPoolName(), PIPE_STATUS_COLFAM, Bytes.fromUTF8(qName),
                getConsistencyLevel(), new ColumnIterator.ColumnOperator() {
                    @Override
                    public boolean execute(Column col) throws Exception {
                        PipeDescriptorImpl pipeDesc = pipeDescFactory.createInstance(qName, col);
                        if (!pipeDesc.isFinishedAndEmpty()) {
                            pipeDescList.add(pipeDesc);
                        }

                        return pipeDescList.size() < maxNumPipeDescs;
                    }
                });

        return pipeDescList;
    }

    // public List<PipeDescriptorImpl> getAllPipes(String qName, int
    // maxNumPipeDescs) throws Exception {
    // final GetAllPipesResult res = new GetAllPipesResult();
    // KeyRowIterator krIter = new KeyRowIterator();
    // krIter.doIt(getQueuePool().getPoolName(), PIPE_STATUS_COLFAM, 1000,
    // getConsistencyLevel(),
    // new KeyRowIterator.KeyOperator() {
    // @Override
    // public void execute(Bytes key, List<Column> colList) throws Exception {
    // res.addStatus(col.getValue());
    // }
    // });
    //
    // Selector s = Pelops.createSelector(getQueuePool().getPoolName());
    // SlicePredicate pred = Selector.newColumnsPredicateAll(false,
    // maxNumPipeDescs);
    // List<Column> colList =
    // s.getColumnsFromRow(PIPE_STATUS_COLFAM, Bytes.fromUTF8(qName), pred,
    // getConsistencyLevel());
    //
    // List<PipeDescriptorImpl> pipeDescList = new
    // ArrayList<PipeDescriptorImpl>(colList.size());
    // for (Column col : colList) {
    // pipeDescList.add(pipeDescFactory.createInstance(qName, col));
    // }
    // return pipeDescList;
    // }

    private CountResult getCountOfMsgsAndStatus(String qName, final String colFamName) throws Exception {
        final CountResult result = new CountResult();
        final SlicePredicate pred = Selector.newColumnsPredicateAll(false, 10000);

        // TODO:BTB currently not removing columns from this CF, which would
        // speed this up after compaction

        ColumnIterator rawMsgColIter = new ColumnIterator();
        rawMsgColIter.doIt(getQueuePool().getPoolName(), PIPE_STATUS_COLFAM, Bytes.fromUTF8(qName),
                getConsistencyLevel(), new ColumnIterator.ColumnOperator() {
                    @Override
                    public boolean execute(Column col) throws Exception {
                        logger.info("working on pipe descriptor : " + UUID.nameUUIDFromBytes(col.getName()));
                        String status = new String(col.getValue());
                        result.addStatus(status);
                        Selector s = Pelops.createSelector(getQueuePool().getPoolName());
                        int msgCount =
                                s.getColumnCount(colFamName, Bytes.fromBytes(col.getName()), pred,
                                        getConsistencyLevel());
                        result.totalMsgCount += msgCount;
                        logger.info(result.numPipeDescriptors + " : status = " + status + ", msgCount = " + msgCount);
                        return true;
                    }
                });

        return result;
    }

    public CountResult getCountOfWaitingMsgs(String qName) throws Exception {
        return getCountOfMsgsAndStatus(qName, formatWaitingColFamName(qName));
    }

    public CountResult getCountOfDeliveredMsgs(String qName) throws Exception {
        return getCountOfMsgsAndStatus(qName, formatDeliveredColFamName(qName));
    }

    public class CountResult {
        public int numPipeDescriptors;
        public int totalMsgCount;
        public Map<String, Integer> statusCounts = new HashMap<String, Integer>();

        public void addStatus(String status) {
            numPipeDescriptors++;
            Integer count = statusCounts.get(status);
            if (null == count) {
                count = new Integer(0);
            }
            statusCounts.put(status, Integer.valueOf(count + 1));
        }

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
