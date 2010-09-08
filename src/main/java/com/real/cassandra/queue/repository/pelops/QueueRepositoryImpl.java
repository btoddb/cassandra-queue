package com.real.cassandra.queue.repository.pelops;

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
import org.apache.cassandra.utils.UUIDGen;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.ColumnFamilyManager;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQMsgFactory;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueDescriptorFactory;
import com.real.cassandra.queue.pipes.PipeDescriptorFactory;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.pipes.PipeStatusFactory;
import com.real.cassandra.queue.repository.ColumnIterator;
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

    private final PelopsPool systemPool;
    private PelopsPool queuePool;

    private QueueDescriptorFactory qDescFactory = new QueueDescriptorFactory();
    private PipeDescriptorFactory pipeDescFactory;
    private CassQMsgFactory qMsgFactory = new CassQMsgFactory();
    private PipeStatusFactory pipeStatusFactory = new PipeStatusFactory();

    public QueueRepositoryImpl(PelopsPool systemPool, PelopsPool queuePool, int replicationFactor,
            ConsistencyLevel consistencyLevel) {
        super(replicationFactor, consistencyLevel);
        this.systemPool = systemPool;
        this.queuePool = queuePool;
        pipeDescFactory = new PipeDescriptorFactory(this);
    }

    /**
     * Perform default initialization of the repository.
     * 
     * @throws Exception
     */
    public void init() throws Exception {
        initKeyspace(false);
    }

    /**
     * Create required column families for this new queue and initialize
     * descriptor.
     * 
     * @param cq
     * @throws Exception
     */
    @Override
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

        return createQueueDescriptor(qName, maxPushTimePerPipe, maxPushesPerPipe, maxPopWidth, popPipeRefreshDelay);
    }

    @Override
    public void truncateQueueData(CassQueueImpl cq) throws Exception {
        String qName = cq.getName();

        ColumnFamilyManager colFamMgr =
                Pelops.createColumnFamilyManager(getSystemPool().getCluster(), QUEUE_KEYSPACE_NAME);

        try {
            colFamMgr.truncateColumnFamily(formatWaitingColFamName(qName));
        }
        catch (Exception e) {
            logger.info("exception while trying to drop column family, " + formatWaitingColFamName(qName));
        }

        try {
            colFamMgr.truncateColumnFamily(formatDeliveredColFamName(qName));
        }
        catch (Exception e) {
            logger.info("exception while trying to drop column family, " + formatDeliveredColFamName(qName));
        }

        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        m.removeRow(PIPE_STATUS_COLFAM, Bytes.fromUTF8(qName), getConsistencyLevel());

        // createQueueDescriptor(cq.getName(), cq.getMaxPushTimePerPipe(),
        // cq.getMaxPushesPerPipe(), cq.getMaxPopWidth(),
        // cq.getPopPipeRefreshDelay());
    }

    @Override
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

    private QueueDescriptor createQueueDescriptor(String qName, long maxPushTimePerPipe, int maxPushesPerPipe,
            int maxPopWidth, long popPipeRefreshDelay) throws Exception {
        QueueDescriptor qDesc = getQueueDescriptor(qName);
        if (null != qDesc) {
            if (qDesc.getMaxPushesPerPipe() != maxPushesPerPipe || qDesc.getMaxPushTimeOfPipe() != maxPushTimePerPipe
                    || qDesc.getMaxPopWidth() != maxPopWidth) {
                throw new IllegalArgumentException(
                        "Queue Descriptor already exists and you passed in maxPushesPerPipe and/or maxPushTimeOfPipe that does not match what is already there");
            }
            return qDesc;
        }

        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());

        Column col = m.newColumn(QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE, Bytes.fromLong(maxPushTimePerPipe));
        m.writeColumn(QUEUE_DESCRIPTORS_COLFAM, qName, col);

        col = m.newColumn(QDESC_COLNAME_MAX_PUSHES_PER_PIPE, Bytes.fromInt(maxPushesPerPipe));
        m.writeColumn(QUEUE_DESCRIPTORS_COLFAM, qName, col);

        col = m.newColumn(QDESC_COLNAME_MAX_POP_WIDTH, Bytes.fromInt(maxPopWidth));
        m.writeColumn(QUEUE_DESCRIPTORS_COLFAM, qName, col);

        col = m.newColumn(QDESC_COLNAME_POP_PIPE_REFRESH_DELAY, Bytes.fromLong(popPipeRefreshDelay));
        m.writeColumn(QUEUE_DESCRIPTORS_COLFAM, qName, col);

        m.execute(getConsistencyLevel());

        return qDescFactory.createInstance(qName, maxPushTimePerPipe, maxPushesPerPipe);
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
    @Override
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

    @Override
    public void createPipeDescriptor(String qName, UUID pipeId, String pipeStatus) throws Exception {
        String rawStatus = pipeStatusFactory.createInstance(new PipeStatus(pipeStatus, 0));

        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        Column col = m.newColumn(Bytes.fromUuid(pipeId), Bytes.fromUTF8(rawStatus));
        m.writeColumn(PIPE_STATUS_COLFAM, qName, col);
        m.execute(getConsistencyLevel());
    }

    @Override
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
    @Override
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

    @Override
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

    static String formatWaitingColFamName(String qName) {
        return qName + WAITING_COLFAM_SUFFIX;
    }

    static String formatDeliveredColFamName(String qName) {
        return qName + PENDING_COMMIT_COLFAM_SUFFIX;
    }

    @Override
    public String dropKeyspace() throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(getSystemPool().getCluster());
        return ksMgr.dropKeyspace(QUEUE_KEYSPACE_NAME);
    }

    @Override
    public boolean isKeyspaceExists() throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(getSystemPool().getCluster());
        Set<String> ksNameSet = ksMgr.getKeyspaceNames();
        return null != ksNameSet && ksNameSet.contains(QUEUE_KEYSPACE_NAME);
    }

    @Override
    public String createKeyspace(KsDef ksDef) throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(getSystemPool().getCluster());
        return ksMgr.addKeyspace(ksDef);
    }

    @Override
    public KsDef getKeyspaceDefinition() throws Exception {
        KeyspaceManager ksMgr = Pelops.createKeyspaceManager(getSystemPool().getCluster());
        KsDef ksDef = ksMgr.getKeyspaceSchema(QueueRepositoryImpl.QUEUE_KEYSPACE_NAME);
        return ksDef;
    }

    @Override
    public CassQMsg getOldestMsgFromDeliveredPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        return getOldestMsgFromPipe(formatDeliveredColFamName(pipeDesc.getQName()), pipeDesc);
    }

    @Override
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

    @Override
    public List<CassQMsg> getDeliveredMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxMsgs) throws Exception {
        return getMessagesFromPipe(formatDeliveredColFamName(pipeDesc.getQName()), pipeDesc, maxMsgs);
    }

    @Override
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

    @Override
    public void removeMsgFromDeliveredPipe(CassQMsg qMsg) throws Exception {
        PipeDescriptorImpl pipeDesc = qMsg.getPipeDescriptor();
        removeMsgFromPipe(formatDeliveredColFamName(pipeDesc.getQName()), pipeDesc, qMsg);
    }

    @Override
    public void removeMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc, CassQMsg qMsg) throws Exception {
        removeMsgFromPipe(formatWaitingColFamName(pipeDesc.getQName()), pipeDesc, qMsg);
    }

    private void removeMsgFromPipe(String colFamName, PipeDescriptorImpl pipeDesc, CassQMsg qMsg) throws Exception {
        Mutator m = Pelops.createMutator(getQueuePool().getPoolName());
        m.deleteColumn(colFamName, Bytes.fromUuid(pipeDesc.getPipeId()), Bytes.fromUuid(qMsg.getMsgId()));
        m.execute(getConsistencyLevel());
    }

    @Override
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

    @Override
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
                        logger.info("working on pipe descriptor : " + UUIDGen.makeType1UUID(col.getName()));
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

    @Override
    public CountResult getCountOfWaitingMsgs(String qName) throws Exception {
        return getCountOfMsgsAndStatus(qName, formatWaitingColFamName(qName));
    }

    @Override
    public CountResult getCountOfPendingCommitMsgs(String qName) throws Exception {
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

    public PelopsPool getSystemPool() {
        return systemPool;
    }

    public PelopsPool getQueuePool() {
        return queuePool;
    }

    @Override
    protected String getSchemaVersion() throws Exception {
        return null;
    }
}
