package com.real.cassandra.queue.repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQMsgFactory;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueStats;
import com.real.cassandra.queue.QueueStatsFactoryImpl;
import com.real.cassandra.queue.model.MessageDescriptor;
import com.real.cassandra.queue.pipes.PipeDescriptorFactory;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.utils.DoubleSerializer;
import com.real.cassandra.queue.utils.UuidGenerator;
import com.real.hom.EntityManager;

public class QueueRepositoryImpl {
    private static Logger logger = LoggerFactory.getLogger(QueueRepositoryImpl.class);

    private static final long MAX_WAIT_SCHEMA_SYNC = 60000;
    public static final QuorumAllConsistencyLevelPolicy consistencyLevelPolicy = new QuorumAllConsistencyLevelPolicy();

    public static final String QUEUE_POOL_NAME = "queuePool";
    public static final String SYSTEM_POOL_NAME = "mySystemPool";
    public static final String QUEUE_NAME = "myTestQueue";

    public static final String QUEUE_KEYSPACE_NAME = "Queues";

    public static final String QUEUE_DESCRIPTORS_COLFAM = "QueueDescriptors";
    public static final String QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE = "maxPushTimeOfPipe";
    public static final String QDESC_COLNAME_MAX_PUSHES_PER_PIPE = "maxPushesPerPipe";

    public static final String QUEUE_STATS_COLFAM = "QueueStats";
    public static final String QSTATS_COLNAME_TOTAL_PUSHES = "totalPushes";
    public static final String QSTATS_COLNAME_TOTAL_POPS = "totalPops";
    public static final String QSTATS_COLNAME_RECENT_PUSHES_PER_SEC = "recentPushesPerSec";
    public static final String QSTATS_COLNAME_RECENT_POPS_PER_SEC = "recentPopsPerSec";

    public static final String QUEUE_PIPE_CNXN_COLFAM = "QueuePipeCnxn";

    public static final String PIPE_DESCRIPTOR_COLFAM = "PipeDescriptors";
    public static final String PDESC_COLNAME_QUEUE_NAME = "qName";
    public static final String PDESC_COLNAME_POP_STATUS = "popStatus";
    public static final String PDESC_COLNAME_PUSH_STATUS = "pushStatus";
    public static final String PDESC_COLNAME_PUSH_COUNT = "pushCount";
    public static final String PDESC_COLNAME_POP_COUNT = "popCount";
    public static final String PDESC_COLNAME_PUSH_START_TIMESTAMP = "pushStartTs";
    public static final String PDESC_COLNAME_POP_OWNER_ID = "popOwnerId";
    public static final String PDESC_COLNAME_POP_OWNER_TIMESTAMP = "popOwnerTs";

    public static final String MSG_DESCRIPTOR_COLFAM = "MessageDescriptors";
    public static final String MDESC_COLNAME_POP_TIMESTAMP = "popTimestamp";

    protected static final String WAITING_COLFAM_SUFFIX = "_Waiting";
    protected static final String PENDING_COLFAM_SUFFIX = "_Pending";
    protected static final int GC_GRACE_SECS = 86400; // one day

    protected static final int MAX_QUEUE_DESCRIPTOR_COLUMNS = 100;
    protected static final int MAX_PIPE_DESCRIPTOR_COLUMNS = 100;

    public static final String SYSTEM_KEYSPACE_NAME = "system";
    public static final String STRATEGY_CLASS_NAME = "org.apache.cassandra.locator.SimpleStrategy";

    protected PipeDescriptorFactory pipeDescFactory;
    protected QueueStatsFactoryImpl qStatsFactory = new QueueStatsFactoryImpl();
    protected CassQMsgFactory qMsgFactory = new CassQMsgFactory();

    private Cluster cluster;
    private Keyspace keyspace;
    private final int replicationFactor;
    private EntityManager entityMgr;

    public QueueRepositoryImpl(Cluster cluster, int replicationFactor, Keyspace keyspace, EntityManager entityMgr) {
        this.cluster = cluster;
        this.replicationFactor = replicationFactor;
        this.keyspace = keyspace;
        this.entityMgr = entityMgr;

        this.pipeDescFactory = new PipeDescriptorFactory();
    }

    /**
     * Creates the queue descriptor if doesn't exists, otherwise uses values
     * from database disregarding the parameters passed from client. Issues
     * warning if parameters don't match database.
     * 
     * @param qName
     * @param maxPushTimePerPipe
     * @param maxPushesPerPipe
     * @return
     */
    public QueueDescriptor createQueueIfDoesntExist(String qName, long maxPushTimePerPipe, int maxPushesPerPipe,
            long transactionTimeout) {
        CfDef colFamDef =
                new CfDef(QUEUE_KEYSPACE_NAME, formatWaitingColFamName(qName)).setComparator_type("TimeUUIDType")
                        .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS);
        String ver = null;
        try {
            ver = createColumnFamily(colFamDef);
            waitForSchemaSync(ver);
        }
        catch (Exception e) {
            logger.info("exception while trying to create column family, " + colFamDef.getName()
                    + " - possibly already exists and is OK");
        }

        colFamDef =
                new CfDef(QUEUE_KEYSPACE_NAME, formatPendingColFamName(qName)).setComparator_type("TimeUUIDType")
                        .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS);
        try {
            ver = createColumnFamily(colFamDef);
            waitForSchemaSync(ver);
        }
        catch (Exception e) {
            logger.info("exception while trying to create column family, " + colFamDef.getName()
                    + " - possibly already exists and is OK");
        }

        return createQueueDescriptorIfNotExists(qName, maxPushTimePerPipe, maxPushesPerPipe, transactionTimeout);
    }

    private void waitForSchemaSync(String newVer) {
        if (null == newVer || newVer.isEmpty()) {
            throw new IllegalArgumentException("version cannot be null or empty");
        }

        long start = System.currentTimeMillis();
        while (!isSchemaInSync(newVer) && (System.currentTimeMillis() - start < MAX_WAIT_SCHEMA_SYNC)) {
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
        logger.info("Waited {}ms to sync schema", System.currentTimeMillis() - start);
    }

    private QueueDescriptor createQueueDescriptorIfNotExists(String qName, long maxPushTimePerPipe,
            int maxPushesPerPipe, long transactionTimeout) {
        QueueDescriptor qDesc = getQueueDescriptor(qName);
        if (null == qDesc) {
            qDesc = new QueueDescriptor(qName);
            qDesc.setMaxPushesPerPipe(maxPushesPerPipe);
            qDesc.setMaxPushTimePerPipe(maxPushTimePerPipe);
            qDesc.setTransactionTimeout(transactionTimeout);
            qDesc = entityMgr.save(qDesc);
        }
        return qDesc;
    }

    public Set<QueueDescriptor> getQueueDescriptors() {
        RangeSlicesQuery<String, String, byte[]> q =
                HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
                        BytesArraySerializer.get());
        q.setRange("", "", false, 100);
        q.setKeys("", "");
        q.setColumnFamily(QUEUE_DESCRIPTORS_COLFAM);

        OrderedRows<String, String, byte[]> r = q.execute().get();

        Set<QueueDescriptor> queueDescriptors; // transformed result set

        if (r == null || r.getCount() < 1) {
            queueDescriptors = Collections.emptySet();
        }
        else {
            List<Row<String, String, byte[]>> rowList = r.getList();
            queueDescriptors = new HashSet<QueueDescriptor>(rowList.size());
            for (Row<String, String, byte[]> row : rowList) {
                queueDescriptors.add(entityMgr.load(QueueDescriptor.class, row.getKey(), row.getColumnSlice()));
            }
        }

        return queueDescriptors;
    }

    public QueueDescriptor getQueueDescriptor(String qName) {
        return entityMgr.load(QueueDescriptor.class, qName);
    }

    public String createKeyspace(final KsDef ksDef) {
        return cluster.addKeyspace(new ThriftKsDef(ksDef));
    }

    protected Map<String, List<String>> getSchemaVersionMap() {
        Operation<Map<String, List<String>>> op = new Operation<Map<String, List<String>>>(OperationType.META_READ) {
            @Override
            public Map<String, List<String>> execute(Client cassandra) throws HectorException {
                try {
                    return cassandra.describe_schema_versions();
                }
                catch (Throwable e) {
                    throw new RuntimeException(e);
                }

            }
        };
        cluster.getConnectionManager().operateWithFailover(op);
        return op.getResult();
    }

    protected String dropKeyspace() {
        Operation<String> op = new Operation<String>(OperationType.META_WRITE) {
            @Override
            public String execute(Client cassandra) throws HectorException {
                try {
                    return cassandra.system_drop_keyspace(QUEUE_KEYSPACE_NAME);
                }
                catch (Throwable e) {
                    throw new RuntimeException(e);
                }

            }
        };
        cluster.getConnectionManager().operateWithFailover(op);
        return op.getResult();
    }

    protected boolean isKeyspaceExists() {
        List<KeyspaceDefinition> ksDefList = cluster.describeKeyspaces();
        for (KeyspaceDefinition ksDef : ksDefList) {
            if (ksDef.getName().equals(QUEUE_KEYSPACE_NAME)) {
                return true;
            }
        }

        return false;
    }

    public CassQMsg insertMsg(PipeDescriptorImpl pipeDesc, UUID msgId, byte[] msgData) {
        // save the message descriptor
        MessageDescriptor msgDesc = new MessageDescriptor();
        msgDesc.setMsgId(msgId);
        msgDesc.setPayload(msgData);
        msgDesc.setCreateTimestamp(System.currentTimeMillis());
        entityMgr.save(msgDesc);

        Mutator<UUID> m = HFactory.createMutator(keyspace, UUIDSerializer.get());

        // add insert into waiting
        HColumn<UUID, byte[]> colMsg =
                HFactory.createColumn(msgId, HectorUtils.EMPTY_BYTES, UUIDSerializer.get(), BytesArraySerializer.get());
        m.addInsertion(pipeDesc.getPipeId(), formatWaitingColFamName(pipeDesc.getQName()), colMsg);

        // update push count
        HColumn<String, Integer> colPipeDesc =
                HFactory.createColumn(PDESC_COLNAME_PUSH_COUNT, pipeDesc.getPushCount(), StringSerializer.get(),
                        IntegerSerializer.get());
        m.addInsertion(pipeDesc.getPipeId(), PIPE_DESCRIPTOR_COLFAM, colPipeDesc);

        m.execute();

        return new CassQMsg(pipeDesc, msgId, msgDesc);
    }

    public void updatePipePushStatus(PipeDescriptorImpl pipeDesc, PipeStatus status) {
        Mutator<UUID> m = HFactory.createMutator(keyspace, UUIDSerializer.get());
        HColumn<String, String> col =
                HFactory.createColumn(PDESC_COLNAME_PUSH_STATUS, status.getName(), StringSerializer.get(),
                        StringSerializer.get());
        m.insert(pipeDesc.getPipeId(), PIPE_DESCRIPTOR_COLFAM, col);
    }

    public void updatePipePopStatus(PipeDescriptorImpl pipeDesc, PipeStatus status) {
        Mutator<UUID> m = HFactory.createMutator(keyspace, UUIDSerializer.get());
        HColumn<String, String> col =
                HFactory.createColumn(PDESC_COLNAME_POP_STATUS, status.getName(), StringSerializer.get(),
                        StringSerializer.get());
        m.insert(pipeDesc.getPipeId(), PIPE_DESCRIPTOR_COLFAM, col);
    }

    public void updatePipePopCount(PipeDescriptorImpl pipeDesc, int popCount, MessageDescriptor msgDesc) {
        Mutator<UUID> m = HFactory.createMutator(keyspace, UUIDSerializer.get());

        // update pipe pop count
        m.addInsertion(pipeDesc.getPipeId(), PIPE_DESCRIPTOR_COLFAM, HFactory.createColumn(PDESC_COLNAME_POP_COUNT,
                popCount, StringSerializer.get(), IntegerSerializer.get()));
        m.execute();
    }

    private void removeMsgFromPipe(String colFamName, PipeDescriptorImpl pipeDesc, CassQMsg qMsg) {
        Mutator<UUID> m = HFactory.createMutator(keyspace, UUIDSerializer.get());
        m.delete(pipeDesc.getPipeId(), colFamName, qMsg.getMsgId(), UUIDSerializer.get());
    }

    public List<PipeDescriptorImpl> getOldestPopActivePipes(final String qName, final int maxNumPipeDescs) {
        return getPipesByPushPopStatus(qName, maxNumPipeDescs, null, PipeStatus.ACTIVE);
    }

    public List<PipeDescriptorImpl> getCompletedPipes(final String qName, final int maxNumPipeDescs) {
        return getPipesByPushPopStatus(qName, maxNumPipeDescs, PipeStatus.COMPLETED, PipeStatus.COMPLETED);
    }

    public List<PipeDescriptorImpl> getPushNotActivePipes(final String qName, final int maxNumPipeDescs) {
        return getPipesByPushPopStatus(qName, maxNumPipeDescs, PipeStatus.NOT_ACTIVE, null);
    }

    public List<PipeDescriptorImpl> getPopFinishedPipes(final String qName, final int maxNumPipeDescs) {
        return getPipesByPushPopStatus(qName, maxNumPipeDescs, null, PipeStatus.NOT_ACTIVE);
    }

    public List<PipeDescriptorImpl> getAllPipes(final String qName, final int maxNumPipeDescs) {
        return getPipesByPushPopStatus(qName, maxNumPipeDescs, null, null);
    }

    public List<PipeDescriptorImpl> getPipesByPushPopStatus(final String qName, final int maxNumPipeDescs,
            final PipeStatus pushStatus, final PipeStatus popStatus) {
        final List<PipeDescriptorImpl> pipeDescList = new LinkedList<PipeDescriptorImpl>();

        ColumnIterator rawMsgColIter = new ColumnIterator();
        rawMsgColIter.doIt(cluster, QUEUE_KEYSPACE_NAME, QUEUE_PIPE_CNXN_COLFAM, qName.getBytes(),
                new ColumnIterator.ColumnOperator() {
                    @Override
                    public boolean execute(HColumn<byte[], byte[]> col) {
                        UUID pipeId = UUIDSerializer.get().fromBytes(col.getName());
                        PipeDescriptorImpl pipeDesc = getPipeDescriptor(pipeId);
                        if (null == pipeDesc) {
                            long createTimestamp = LongSerializer.get().fromBytes(col.getValue());
                            // if descriptor and CNXN are out of sync more than
                            // a minute then remove
                            if (60000 < System.currentTimeMillis() - createTimestamp) {
                                logger.error("pipeId ({}, {}) does not have a descriptor and is expired", pipeId,
                                        createTimestamp);
                                removePipeDescriptor(qName, pipeId);
                            }
                            return true;
                        }

                        if ((null == pushStatus || pipeDesc.getPushStatus().equals(pushStatus))
                                && (null == popStatus || pipeDesc.getPopStatus().equals(popStatus))) {
                            pipeDescList.add(pipeDesc);
                        }

                        return pipeDescList.size() < maxNumPipeDescs;
                    }
                });

        return pipeDescList;
    }

    public List<CassQMsg> getOldestMsgsFromPendingPipe(PipeDescriptorImpl pipeDesc, int maxMsgs) {
        return getOldestMsgsFromPipe(formatPendingColFamName(pipeDesc.getQName()), pipeDesc, maxMsgs);
    }

    private List<CassQMsg> getOldestMsgsFromPipe(String colFameName, PipeDescriptorImpl pipeDesc, int maxMsgs) {
        SliceQuery<UUID, UUID, byte[]> q =
                HFactory.createSliceQuery(keyspace, UUIDSerializer.get(), UUIDSerializer.get(), BytesArraySerializer
                        .get());
        q.setColumnFamily(colFameName);
        q.setKey(pipeDesc.getPipeId());
        q.setRange(null, null, false, maxMsgs);
        QueryResult<ColumnSlice<UUID, byte[]>> res = q.execute();

        ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(maxMsgs);
        for (HColumn<UUID, byte[]> col : res.get().getColumns()) {
            UUID msgId = col.getName();
            CassQMsg qMsg = new CassQMsg(pipeDesc, msgId, entityMgr.load(MessageDescriptor.class, msgId));
            msgList.add(qMsg);
        }
        return msgList;
    }

    public List<CassQMsg> getOldestMsgsFromQueue(final String qName, final int maxMsgs) {
        final LinkedList<CassQMsg> result = new LinkedList<CassQMsg>();
        final String colFamName = formatWaitingColFamName(qName);

        ColumnIterator rawMsgColIter = new ColumnIterator();
        rawMsgColIter.doIt(cluster, QUEUE_KEYSPACE_NAME, QUEUE_PIPE_CNXN_COLFAM, qName.getBytes(),
                new ColumnIterator.ColumnOperator() {
                    @Override
                    public boolean execute(HColumn<byte[], byte[]> col) {
                        UUID pipeId = UUIDSerializer.get().fromBytes(col.getName());
                        PipeDescriptorImpl pipeDesc = getPipeDescriptor(pipeId);
                        if (PipeStatus.ACTIVE.equals(pipeDesc.getPopStatus())) {
                            logger.info("working on pipe descriptor : " + pipeId);
                            result.addAll(getMsgsInPipe(qName, colFamName, pipeDesc, maxMsgs));
                        }
                        return maxMsgs > result.size();
                    }
                });

        return result;
    }

    private List<CassQMsg> getMsgsInPipe(final String qName, final String colFamName,
            final PipeDescriptorImpl pipeDesc, final int maxMsgs) {
        final LinkedList<CassQMsg> result = new LinkedList<CassQMsg>();

        ColumnIterator rawMsgColIter = new ColumnIterator();
        rawMsgColIter.doIt(cluster, QUEUE_KEYSPACE_NAME, colFamName, UUIDGen.decompose(pipeDesc.getPipeId()),
                new ColumnIterator.ColumnOperator() {
                    @Override
                    public boolean execute(HColumn<byte[], byte[]> col) {
                        UUID msgId = UuidGenerator.createInstance(col.getName());
                        MessageDescriptor msgDesc = entityMgr.load(MessageDescriptor.class, msgId);

                        CassQMsg qMsg = new CassQMsg(pipeDesc, msgId, msgDesc);
                        result.add(qMsg);
                        return maxMsgs > result.size();
                    }
                });

        return result;
    }

    /**
     * Move msg from waiting pipe to pending pipe and set 'pop' timestamp for
     * transaction timeout, if needed.
     * 
     * @param qMsg
     */
    public void moveMsgFromWaitingToPendingPipe(CassQMsg qMsg) {
        PipeDescriptorImpl pipeDesc = qMsg.getPipeDescriptor();
        qMsg.getMsgDesc().setPopTimestamp(System.currentTimeMillis());
        String qName = qMsg.getPipeDescriptor().getQName();
        Mutator<UUID> m = HFactory.createMutator(keyspace, UUIDSerializer.get());
        m.addInsertion(pipeDesc.getPipeId(), formatPendingColFamName(qName), HFactory.createColumn(qMsg.getMsgId(),
                HectorUtils.EMPTY_BYTES, UUIDSerializer.get(), BytesArraySerializer.get()));
        m.addInsertion(qMsg.getMsgId(), MSG_DESCRIPTOR_COLFAM, HFactory.createColumn(MDESC_COLNAME_POP_TIMESTAMP, qMsg
                .getMsgDesc().getPopTimestamp(), StringSerializer.get(), LongSerializer.get()));
        m.addDeletion(pipeDesc.getPipeId(), formatWaitingColFamName(qName), qMsg.getMsgId(), UUIDSerializer.get());
        m.execute();
    }

    public void truncateQueuePipeCnxn(CassQueueImpl cq) {
        final Mutator<UUID> m = HFactory.createMutator(keyspace, UUIDSerializer.get());

        // remove all pipes from cnxn colfam
        ColumnIterator rawMsgColIter = new ColumnIterator();
        rawMsgColIter.doIt(cluster, QUEUE_KEYSPACE_NAME, QUEUE_PIPE_CNXN_COLFAM, cq.getName().getBytes(),
                new ColumnIterator.ColumnOperator() {
                    int count = 0;

                    @Override
                    public boolean execute(HColumn<byte[], byte[]> col) {
                        UUID pipeId = UUIDSerializer.get().fromBytes(col.getName());
                        m.addDeletion(pipeId, PIPE_DESCRIPTOR_COLFAM, null, BytesArraySerializer.get());
                        count++;
                        if (count > 10) {
                            m.execute();
                            m.discardPendingMutations();
                            count = 0;
                        }
                        return true;
                    }
                });

        m.execute();
    }

    /**
     * Truncates the data only. Descriptors, Stats, Pipes, all stay as is.
     * 
     * @param cq
     *            Cassandra queue to truncate
     */
    public void truncateQueueData(final CassQueueImpl cq) {
        Operation<Void> op = new Operation<Void>(OperationType.META_WRITE) {
            @Override
            public Void execute(Client cassandra) throws HectorException {
                try {
                    cassandra.set_keyspace(QUEUE_KEYSPACE_NAME);
                    cassandra.truncate(formatWaitingColFamName(cq.getName()));
                    cassandra.truncate(formatPendingColFamName(cq.getName()));
                    return null;
                }
                catch (Throwable e) {
                    throw new RuntimeException(e);
                }

            }
        };
        cluster.getConnectionManager().operateWithFailover(op);
    }

    public void dropQueue(CassQueueImpl cq) {
        cluster.dropColumnFamily(QUEUE_KEYSPACE_NAME, formatWaitingColFamName(cq.getName()));
        cluster.dropColumnFamily(QUEUE_KEYSPACE_NAME, formatPendingColFamName(cq.getName()));

        truncateQueuePipeCnxn(cq);

        Mutator<String> m = HFactory.createMutator(keyspace, StringSerializer.get());
        m.addDeletion(cq.getName(), QUEUE_PIPE_CNXN_COLFAM, null, UUIDSerializer.get());
        m.addDeletion(cq.getName(), QUEUE_STATS_COLFAM, null, UUIDSerializer.get());
        m.execute();
    }

    public CassQMsg getMsg(String qName, PipeDescriptorImpl pipeDesc, UUID msgId) {

        ColumnQuery<UUID, UUID, byte[]> q =
                HFactory.createColumnQuery(keyspace, UUIDSerializer.get(), UUIDSerializer.get(), BytesArraySerializer
                        .get());
        q.setColumnFamily(formatWaitingColFamName(qName));
        q.setKey(pipeDesc.getPipeId());
        q.setName(msgId);
        QueryResult<HColumn<UUID, byte[]>> result = q.execute();
        if (null == result || null == result.get()) {
            return null;
        }

        MessageDescriptor msgDesc = entityMgr.load(MessageDescriptor.class, msgId);
        return new CassQMsg(pipeDesc, msgId, msgDesc);
    }

    public PipeDescriptorImpl createPipeDescriptor(String qName, UUID pipeId) {
        return createPipeDescriptor(qName, pipeId, System.currentTimeMillis());
    }

    public PipeDescriptorImpl createPipeDescriptor(String qName, UUID pipeId, long startTimestamp) {
        PipeDescriptorImpl pipeDesc = pipeDescFactory.createInstance(qName, pipeId);
        pipeDesc.setPushStartTimestamp(startTimestamp);

        Mutator<byte[]> m = HFactory.createMutator(keyspace, BytesArraySerializer.get());

        Set<HColumn<String, byte[]>> colSet = pipeDescFactory.createInstance(pipeDesc);
        byte[] pipeIdAsBytes = UUIDSerializer.get().toBytes(pipeId);
        for (HColumn<String, byte[]> colDesc : colSet) {
            m.addInsertion(pipeIdAsBytes, PIPE_DESCRIPTOR_COLFAM, colDesc);
        }

        HColumn<UUID, Long> colCnxn =
                HFactory.createColumn(pipeId, System.currentTimeMillis(), UUIDSerializer.get(), LongSerializer.get());
        m.addInsertion(StringSerializer.get().toBytes(qName), QUEUE_PIPE_CNXN_COLFAM, colCnxn);

        m.execute();
        return pipeDesc;
    }

    public PipeDescriptorImpl getPipeDescriptor(UUID pipeId) {
        SliceQuery<UUID, String, byte[]> q =
                HFactory.createSliceQuery(keyspace, UUIDSerializer.get(), StringSerializer.get(), BytesArraySerializer
                        .get());
        q.setRange(null, null, false, MAX_PIPE_DESCRIPTOR_COLUMNS);
        q.setColumnFamily(PIPE_DESCRIPTOR_COLFAM);
        q.setKey(pipeId);
        return pipeDescFactory.createInstance(pipeId, q.execute().get());
    }

    protected CountResult getCountOfMsgsAndStatus(String qName, final String colFamName, int maxPageSize) {
        final CountResult result = new CountResult();
        final CountQuery<byte[], byte[]> countQuery =
                HFactory.createCountQuery(keyspace, BytesArraySerializer.get(), BytesArraySerializer.get());
        countQuery.setColumnFamily(colFamName);
        countQuery.setRange(new byte[] {}, new byte[] {}, maxPageSize);

        ColumnIterator rawMsgColIter = new ColumnIterator();
        rawMsgColIter.doIt(cluster, QUEUE_KEYSPACE_NAME, QUEUE_PIPE_CNXN_COLFAM, qName.getBytes(),
                new ColumnIterator.ColumnOperator() {
                    @Override
                    public boolean execute(HColumn<byte[], byte[]> col) {
                        UUID pipeId = UUIDSerializer.get().fromBytes(col.getName());
                        PipeDescriptorImpl pipeDesc = getPipeDescriptor(pipeId);
                        if (null == pipeDesc) {
                            logger.debug("pipeDesc is null : {}", pipeId);
                            return true;
                        }
                        logger.info("working on pipe descriptor : " + pipeDesc.toString());
                        result.addStatus(pipeDesc.getPushStatus(), pipeDesc.getPopStatus());

                        int msgCount = countQuery.setKey(col.getName()).execute().get();

                        result.totalMsgCount += msgCount;
                        logger.info(result.numPipeDescriptors + " : pushStatus = " + pipeDesc.getPushStatus()
                                + ", popStatus = " + pipeDesc.getPopStatus() + ", msgCount = " + msgCount);
                        return true;
                    }
                });

        return result;
    }

    public KeyspaceDefinition getKeyspaceDefinition() {
        KeyspaceDefinition ksDef = cluster.describeKeyspace(QUEUE_KEYSPACE_NAME);
        return ksDef;
    }

    public String createColumnFamily(CfDef colFamDef) {
        logger.debug("creating column family, {}", colFamDef.getName());
        return cluster.addColumnFamily(new ThriftCfDef(colFamDef));
    }

    public void shutdown() {
        // do nothing
    }

    /**
     * Perform default initialization of the repository. Intended use is for
     * spring 'init-method'
     * 
     */
    public void init() {
        initKeyspace(false);
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public boolean isQueueExists(String qName) {
        return null != getQueueDescriptor(qName);
    }

    /**
     * Initialize cassandra server for use with queues.
     * 
     * @param forceRecreate
     *            if true will drop the keyspace and recreate it.
     */
    public void initKeyspace(boolean forceRecreate) {
        String schemaVer = null;
        if (isKeyspaceExists()) {
            if (!forceRecreate) {
                return;
            }
            else {
                schemaVer = dropKeyspace();
                waitForSchemaSync(schemaVer);
            }
        }

        KsDef ksDef = createKeyspaceDefinition();
        schemaVer = createKeyspace(ksDef);
        waitForSchemaSync(schemaVer);
    }

    private KsDef createKeyspaceDefinition() {
        ArrayList<CfDef> cfDefList = new ArrayList<CfDef>(2);
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, QUEUE_DESCRIPTORS_COLFAM).setComparator_type(
                BytesType.class.getSimpleName()).setKey_cache_size(0).setRow_cache_size(1000).setGc_grace_seconds(
                GC_GRACE_SECS));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, QUEUE_STATS_COLFAM).setComparator_type(
                BytesType.class.getSimpleName()).setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(
                GC_GRACE_SECS));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, PIPE_DESCRIPTOR_COLFAM).setComparator_type(
                BytesType.class.getSimpleName()).setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(
                GC_GRACE_SECS));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, QUEUE_PIPE_CNXN_COLFAM).setComparator_type(
                TimeUUIDType.class.getSimpleName()).setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(
                GC_GRACE_SECS));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, MSG_DESCRIPTOR_COLFAM).setComparator_type(
                BytesType.class.getSimpleName()).setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(
                GC_GRACE_SECS));

        return new KsDef(QUEUE_KEYSPACE_NAME, STRATEGY_CLASS_NAME, getReplicationFactor(), cfDefList);
    }

    public boolean isSchemaInSync(String version) {
        Map<String, List<String>> schemaMap = getSchemaVersionMap();
        if (null == schemaMap) {
            return false;
        }

        return null != schemaMap && schemaMap.containsKey(version) && 1 == schemaMap.size();
    }

    public static String formatWaitingColFamName(String qName) {
        return qName + WAITING_COLFAM_SUFFIX;
    }

    public static String formatPendingColFamName(String qName) {
        return qName + PENDING_COLFAM_SUFFIX;
    }

    public CountResult getCountOfWaitingMsgs(String qName, int maxMsgCount) {
        return getCountOfMsgsAndStatus(qName, formatWaitingColFamName(qName), maxMsgCount);
    }

    public CountResult getCountOfPendingCommitMsgs(String qName, int maxMsgCount) {
        return getCountOfMsgsAndStatus(qName, formatPendingColFamName(qName), maxMsgCount);
    }

    public CassQMsg getOldestMsgFromPendingPipe(PipeDescriptorImpl pipeDesc) {
        return getOldestMsgFromPipe(formatPendingColFamName(pipeDesc.getQName()), pipeDesc);
    }

    public CassQMsg getOldestMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc) {
        return getOldestMsgFromPipe(formatWaitingColFamName(pipeDesc.getQName()), pipeDesc);
    }

    private CassQMsg getOldestMsgFromPipe(String colFamName, PipeDescriptorImpl pipeDesc) {
        List<CassQMsg> msgList = getOldestMsgsFromPipe(colFamName, pipeDesc, 1);
        if (null != msgList && !msgList.isEmpty()) {
            return msgList.get(0);
        }
        else {
            return null;
        }
    }

    public List<CassQMsg> getPendingMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxMsgs) {
        return getOldestMsgsFromPipe(formatPendingColFamName(pipeDesc.getQName()), pipeDesc, maxMsgs);
    }

    public List<CassQMsg> getWaitingMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxMsgs) {
        return getOldestMsgsFromPipe(formatWaitingColFamName(pipeDesc.getQName()), pipeDesc, maxMsgs);
    }

    public void removeMsgFromPendingPipe(CassQMsg qMsg) {
        PipeDescriptorImpl pipeDesc = qMsg.getPipeDescriptor();
        removeMsgFromPipe(formatPendingColFamName(pipeDesc.getQName()), pipeDesc, qMsg);
    }

    public void removeMsgFromWaitingPipe(CassQMsg qMsg) {
        PipeDescriptorImpl pipeDesc = qMsg.getPipeDescriptor();
        removeMsgFromPipe(formatWaitingColFamName(pipeDesc.getQName()), pipeDesc, qMsg);
    }

    public class CountResult {
        public int numPipeDescriptors;
        public int totalMsgCount;
        public Map<PipeStatus, Integer> pushStatusCounts = new HashMap<PipeStatus, Integer>();
        public Map<PipeStatus, Integer> popStatusCounts = new HashMap<PipeStatus, Integer>();

        public void addStatus(PipeStatus pushStatus, PipeStatus popStatus) {
            numPipeDescriptors++;

            Integer count = pushStatusCounts.get(pushStatus);
            if (null == count) {
                count = new Integer(0);
            }
            pushStatusCounts.put(pushStatus, Integer.valueOf(count + 1));

            count = popStatusCounts.get(popStatus);
            if (null == count) {
                count = new Integer(0);
            }
            popStatusCounts.put(popStatus, Integer.valueOf(count + 1));
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("CountResult [numPipeDescriptors=");
            builder.append(numPipeDescriptors);
            builder.append(", totalMsgCount=");
            builder.append(totalMsgCount);
            builder.append(", pushStatusCounts=");
            builder.append(pushStatusCounts);
            builder.append(", popStatusCounts=");
            builder.append(popStatusCounts);
            builder.append("]");
            return builder.toString();
        }
    }

    public QueueStats getQueueStats(String qName) {
        SliceQuery<String, String, byte[]> q =
                HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
                        BytesArraySerializer.get());
        q.setRange("", "", false, 100);
        q.setColumnFamily(QUEUE_STATS_COLFAM);
        q.setKey(qName);

        QueueStats qStats = qStatsFactory.createInstance(qName, q.execute().get());
        if (null == qStats) {
            // we always want to return stats, even if they don't exist
            qStats = new QueueStats(qName);
        }
        return qStats;
    }

    public void updateQueueStats(QueueStats qStats) {
        Mutator<String> m = HFactory.createMutator(keyspace, StringSerializer.get());
        m.addInsertion(qStats.getQName(), QUEUE_STATS_COLFAM, HFactory.createColumn(QSTATS_COLNAME_TOTAL_PUSHES, qStats
                .getTotalPushes(), StringSerializer.get(), LongSerializer.get()));
        m.addInsertion(qStats.getQName(), QUEUE_STATS_COLFAM, HFactory.createColumn(QSTATS_COLNAME_TOTAL_POPS, qStats
                .getTotalPops(), StringSerializer.get(), LongSerializer.get()));
        m.addInsertion(qStats.getQName(), QUEUE_STATS_COLFAM, HFactory.createColumn(
                QSTATS_COLNAME_RECENT_PUSHES_PER_SEC, DoubleSerializer.get().toBytes(qStats.getRecentPushesPerSec()),
                StringSerializer.get(), BytesArraySerializer.get()));
        m.addInsertion(qStats.getQName(), QUEUE_STATS_COLFAM, HFactory.createColumn(QSTATS_COLNAME_RECENT_POPS_PER_SEC,
                DoubleSerializer.get().toBytes(qStats.getRecentPopsPerSec()), StringSerializer.get(),
                BytesArraySerializer.get()));
        m.execute();
    }

    public void removePipeDescriptor(PipeDescriptorImpl pipeDesc) {
        removePipeDescriptor(pipeDesc.getQName(), pipeDesc.getPipeId());
    }

    public void removePipeDescriptor(String qName, UUID pipeId) {
        Mutator<byte[]> m = HFactory.createMutator(keyspace, BytesArraySerializer.get());
        m.addDeletion(UUIDSerializer.get().toBytes(pipeId), PIPE_DESCRIPTOR_COLFAM, null, BytesArraySerializer.get());
        m.delete(StringSerializer.get().toBytes(qName), QUEUE_PIPE_CNXN_COLFAM, pipeId, UUIDSerializer.get());
        m.execute();
    }

    public void savePipePopOwner(PipeDescriptorImpl pd, UUID popOwnerId, long timestamp) {
        Mutator<UUID> m = HFactory.createMutator(keyspace, UUIDSerializer.get());
        HColumn<String, UUID> col1 =
                HFactory.createColumn(PDESC_COLNAME_POP_OWNER_ID, popOwnerId, StringSerializer.get(), UUIDSerializer
                        .get());
        m.addInsertion(pd.getPipeId(), PIPE_DESCRIPTOR_COLFAM, col1);
        HColumn<String, Long> col2 =
                HFactory.createColumn(PDESC_COLNAME_POP_OWNER_TIMESTAMP, timestamp, StringSerializer.get(),
                        LongSerializer.get());
        m.addInsertion(pd.getPipeId(), PIPE_DESCRIPTOR_COLFAM, col2);
        m.execute();
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }
}
