package com.real.cassandra.queue.repository.hector;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.model.ColumnSlice;
import me.prettyprint.cassandra.model.CountQuery;
import me.prettyprint.cassandra.model.HColumn;
import me.prettyprint.cassandra.model.KeyspaceOperator;
import me.prettyprint.cassandra.model.Mutator;
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.model.Result;
import me.prettyprint.cassandra.model.SliceQuery;
import me.prettyprint.cassandra.serializers.BytesSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueDescriptorFactoryAbstractImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
import com.real.cassandra.queue.utils.UuidGenerator;

public class QueueRepositoryImpl extends QueueRepositoryAbstractImpl {
    private static Logger logger = LoggerFactory.getLogger(QueueRepositoryImpl.class);

    public static final QuorumAllConsistencyLevelPolicy consistencyLevelPolicy = new QuorumAllConsistencyLevelPolicy();

    private Cluster cluster;
    private QueueDescriptorFactoryImpl qDescFactory;
    private UUIDSerializer uuidSerializer = UUIDSerializer.get();
    private BytesSerializer bytesSerializer = BytesSerializer.get();
    private KeyspaceOperator ko;

    public QueueRepositoryImpl(Cluster cluster, int replicationFactor, KeyspaceOperator ko) {
        super(replicationFactor);
        this.cluster = cluster;
        this.qDescFactory = new QueueDescriptorFactoryImpl();
        this.ko = ko;
    }

    @Override
    public QueueDescriptor getQueueDescriptor(String qName) throws Exception {
        StringSerializer se = StringSerializer.get();
        BytesSerializer be = BytesSerializer.get();

        SliceQuery<String, String, byte[]> q = HFactory.createSliceQuery(ko, se, se, be);
        q.setRange("", "", false, 100);
        q.setColumnFamily(QUEUE_DESCRIPTORS_COLFAM);
        q.setKey(qName);
        Result<ColumnSlice<String, byte[]>> r = q.execute();
        return qDescFactory.createInstance(qName, r);
    }

    @Override
    protected void createQueueDescriptor(String qName, long maxPushTimePerPipe, int maxPushesPerPipe, int maxPopWidth,
            long popPipeRefreshDelay) throws Exception {

        StringSerializer se = StringSerializer.get();
        IntegerSerializer ie = IntegerSerializer.get();
        LongSerializer le = LongSerializer.get();

        // insert value
        Mutator<String> m = HFactory.createMutator(ko, StringSerializer.get());
        m.addInsertion(qName, QUEUE_DESCRIPTORS_COLFAM,
                HFactory.createColumn(QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE, maxPushTimePerPipe, se, le));
        m.addInsertion(qName, QUEUE_DESCRIPTORS_COLFAM,
                HFactory.createColumn(QDESC_COLNAME_MAX_PUSHES_PER_PIPE, maxPushesPerPipe, se, ie));
        m.addInsertion(qName, QUEUE_DESCRIPTORS_COLFAM,
                HFactory.createColumn(QDESC_COLNAME_MAX_POP_WIDTH, maxPopWidth, se, ie));
        m.addInsertion(qName, QUEUE_DESCRIPTORS_COLFAM,
                HFactory.createColumn(QDESC_COLNAME_POP_PIPE_REFRESH_DELAY, popPipeRefreshDelay, se, le));
        m.execute();
    }

    @Override
    public String createKeyspace(KsDef ksDef) throws Exception {
        CassandraClient client = cluster.borrowClient();
        try {
            Client thriftClient = client.getCassandra();
            return thriftClient.system_add_keyspace(ksDef);
        }
        finally {
            cluster.releaseClient(client);
        }
    }

    @Override
    protected Map<String, List<String>> getSchemaVersionMap() throws Exception {
        Map<String, List<String>> schemaMap;
        CassandraClient client = cluster.borrowClient();
        try {
            Client thriftClient = client.getCassandra();
            schemaMap = thriftClient.describe_schema_versions();
        }
        finally {
            cluster.releaseClient(client);
        }

        return schemaMap;
    }

    @Override
    protected String dropKeyspace() throws Exception {
        CassandraClient client = cluster.borrowClient();
        try {
            Client thriftClient = client.getCassandra();
            return thriftClient.system_drop_keyspace(QUEUE_KEYSPACE_NAME);
        }
        finally {
            cluster.releaseClient(client);
        }

    }

    @Override
    protected boolean isKeyspaceExists() throws Exception {
        List<KsDef> ksDefList = cluster.describeKeyspaces();
        for (KsDef ksDef : ksDefList) {
            if (ksDef.getName().equals(QUEUE_KEYSPACE_NAME)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void insert(PipeDescriptorImpl pipeDesc, UUID msgId, String msgData) throws Exception {
        Mutator<byte[]> m = HFactory.createMutator(ko, bytesSerializer);

        // add insert into waiting
        HColumn<UUID, byte[]> col = HFactory.createColumn(msgId, msgData.getBytes(), uuidSerializer, bytesSerializer);
        m.addInsertion(uuidSerializer.toBytes(pipeDesc.getPipeId()), formatWaitingColFamName(pipeDesc.getQName()), col);

        // update msg count
        logger.debug("updating pipe descriptor to increase msg count : {}", pipeDesc);
        String rawStatus =
                pipeStatusFactory.createInstance(pipeStatusFactory.createInstance(pipeDesc.getStatus(),
                        pipeDesc.getMsgCount(), pipeDesc.getStartTimestamp()));
        col = HFactory.createColumn(pipeDesc.getPipeId(), rawStatus.getBytes(), uuidSerializer, bytesSerializer);
        m.addInsertion(pipeDesc.getQName().getBytes(), PIPE_STATUS_COLFAM, col);

        m.execute();
    }

    @Override
    public void setPipeDescriptorStatus(PipeDescriptorImpl pipeDesc, String pipeStatus) throws Exception {
        String rawStatus =
                pipeStatusFactory.createInstance(pipeStatusFactory.createInstance(pipeStatus, pipeDesc.getMsgCount(),
                        pipeDesc.getStartTimestamp()));
        KeyspaceOperator ko = HFactory.createKeyspaceOperator(QUEUE_KEYSPACE_NAME, cluster);
        Mutator<String> m = HFactory.createMutator(ko, StringSerializer.get());
        m.insert(pipeDesc.getQName(), PIPE_STATUS_COLFAM,
                HFactory.createColumn(pipeDesc.getPipeId(), rawStatus, UUIDSerializer.get(), StringSerializer.get()));
    }

    @Override
    protected void removeMsgFromPipe(String colFamName, PipeDescriptorImpl pipeDesc, CassQMsg qMsg) throws Exception {
        Mutator<UUID> m = HFactory.createMutator(ko, uuidSerializer);
        m.delete(pipeDesc.getPipeId(), colFamName, qMsg.getMsgId(), uuidSerializer);
    }

    @Override
    public List<PipeDescriptorImpl> getOldestNonEmptyPipes(final String qName, final int maxNumPipeDescs)
            throws Exception {
        final List<PipeDescriptorImpl> pipeDescList = new LinkedList<PipeDescriptorImpl>();

        ColumnIterator rawMsgColIter = new ColumnIterator();
        rawMsgColIter.doIt(cluster, QUEUE_KEYSPACE_NAME, PIPE_STATUS_COLFAM, qName.getBytes(),
                new ColumnIterator.ColumnOperator() {
                    @Override
                    public boolean execute(HColumn<byte[], byte[]> col) throws Exception {
                        PipeStatus ps = pipeStatusFactory.createInstance(new String(col.getValue()));
                        PipeDescriptorImpl pipeDesc =
                                pipeDescFactory.createInstance(qName, UuidGenerator.createInstance(col.getName()),
                                        ps.getStatus(), ps.getPushCount(), ps.getStartTimestamp());
                        if (!pipeDesc.isFinishedAndEmpty()) {
                            pipeDescList.add(pipeDesc);
                        }

                        return pipeDescList.size() < maxNumPipeDescs;
                    }
                });

        return pipeDescList;
    }

    @Override
    protected List<CassQMsg> getOldestMsgsFromPipe(String colFameName, PipeDescriptorImpl pipeDesc, int maxMsgs)
            throws Exception {
        SliceQuery<UUID, UUID, byte[]> q =
                HFactory.createSliceQuery(ko, uuidSerializer, uuidSerializer, bytesSerializer);
        q.setColumnFamily(colFameName);
        q.setKey(pipeDesc.getPipeId());
        q.setRange(null, null, false, maxMsgs);
        Result<ColumnSlice<UUID, byte[]>> res = q.execute();

        ArrayList<CassQMsg> msgList = new ArrayList<CassQMsg>(maxMsgs);
        for (HColumn<UUID, byte[]> col : res.get().getColumns()) {
            msgList.add(qMsgFactory.createInstance(pipeDesc, col));
        }
        return msgList;
    }

    @Override
    public void moveMsgFromWaitingToPendingPipe(CassQMsg qMsg) throws Exception {
        PipeDescriptorImpl pipeDesc = qMsg.getPipeDescriptor();
        String qName = qMsg.getPipeDescriptor().getQName();
        Mutator<UUID> m = HFactory.createMutator(ko, uuidSerializer);
        HColumn<UUID, byte[]> col =
                HFactory.createColumn(qMsg.getMsgId(), qMsg.getMsgData().getBytes(), uuidSerializer, bytesSerializer);
        m.addInsertion(pipeDesc.getPipeId(), formatPendingColFamName(qName), col);
        m.addDeletion(pipeDesc.getPipeId(), formatWaitingColFamName(qName), qMsg.getMsgId(), uuidSerializer);
        m.execute();
    }

    @Override
    public void truncateQueueData(CassQueueImpl cq) throws Exception {
        String qName = cq.getName();

        CassandraClient client = cluster.borrowClient();
        try {
            Client thriftClient = client.getCassandra();
            thriftClient.set_keyspace(QUEUE_KEYSPACE_NAME);
            thriftClient.truncate(formatWaitingColFamName(qName));
            thriftClient.truncate(formatPendingColFamName(qName));
        }
        finally {
            cluster.releaseClient(client);
        }

        Mutator<String> m = HFactory.createMutator(ko, StringSerializer.get());
        m.delete(qName, PIPE_STATUS_COLFAM, null, null);
        // TODO : remove QUEUE_STATS when created
    }

    @Override
    public void dropQueue(CassQueueImpl cq) throws Exception {
        String qName = cq.getName();

        CassandraClient client = cluster.borrowClient();
        try {
            Client thriftClient = client.getCassandra();
            thriftClient.system_drop_column_family(formatWaitingColFamName(qName));
            thriftClient.system_drop_column_family(formatPendingColFamName(qName));
        }
        finally {
            cluster.releaseClient(client);
        }

        Mutator<String> m = HFactory.createMutator(ko, StringSerializer.get());
        m.delete(qName, QUEUE_DESCRIPTORS_COLFAM, null, null);
        m.delete(qName, PIPE_STATUS_COLFAM, null, null);
        // TODO : remove QUEUE_STATS when created
    }

    @Override
    public CassQMsg getMsg(String qName, PipeDescriptorImpl pipeDesc, UUID msgId) throws Exception {
        ColumnQuery<UUID, UUID, byte[]> q =
                HFactory.createColumnQuery(ko, uuidSerializer, uuidSerializer, bytesSerializer);
        q.setColumnFamily(formatWaitingColFamName(qName));
        q.setKey(pipeDesc.getPipeId());
        q.setName(msgId);
        Result<HColumn<UUID, byte[]>> result = q.execute();
        return qMsgFactory.createInstance(pipeDesc, result.get());
    }

    @Override
    public void createPipeDescriptor(String qName, UUID pipeId, String pipeStatus, long startTimestamp)
            throws Exception {
        String rawStatus =
                pipeStatusFactory.createInstance(pipeStatusFactory.createInstance(pipeStatus, 0, startTimestamp));
        Mutator<String> m = HFactory.createMutator(ko, StringSerializer.get());
        HColumn<UUID, String> col = HFactory.createColumn(pipeId, rawStatus, uuidSerializer, StringSerializer.get());
        m.insert(qName, PIPE_STATUS_COLFAM, col);
    }

    @Override
    public PipeDescriptorImpl getPipeDescriptor(String qName, UUID pipeId) throws Exception {
        ColumnQuery<byte[], UUID, String> q =
                HFactory.createColumnQuery(ko, bytesSerializer, uuidSerializer, StringSerializer.get());
        q.setColumnFamily(PIPE_STATUS_COLFAM);
        q.setKey(qName.getBytes());
        q.setName(pipeId);
        Result<HColumn<UUID, String>> result = q.execute();
        if (null != result && null != result.get()) {
            return pipeDescFactory.createInstance(qName, result.get());
        }
        return null;
    }

    @Override
    protected CountResult getCountOfMsgsAndStatus(String qName, final String colFamName) throws Exception {
        final CountResult result = new CountResult();
        final BytesSerializer bs = BytesSerializer.get();
        final CountQuery<byte[], byte[]> countQuery = HFactory.createCountQuery(ko, bs, bs);
        countQuery.setColumnFamily(colFamName);

        // TODO:BTB currently not removing columns from this CF, which would
        // speed this up after compaction

        ColumnIterator rawMsgColIter = new ColumnIterator();
        rawMsgColIter.doIt(cluster, QUEUE_KEYSPACE_NAME, PIPE_STATUS_COLFAM, qName.getBytes(),
                new ColumnIterator.ColumnOperator() {
                    @Override
                    public boolean execute(HColumn<byte[], byte[]> col) throws Exception {
                        logger.info("working on pipe descriptor : " + UUIDGen.makeType1UUID(col.getName()));
                        String status = new String(col.getValue());
                        result.addStatus(status);

                        int msgCount = countQuery.setKey(col.getValue()).execute().get();

                        result.totalMsgCount += msgCount;
                        logger.info(result.numPipeDescriptors + " : status = " + status + ", msgCount = " + msgCount);
                        return true;
                    }
                });

        return result;
    }

    @Override
    public KsDef getKeyspaceDefinition() throws Exception {
        KsDef ksDef = cluster.describeKeyspace(QUEUE_KEYSPACE_NAME);
        return ksDef;
    }

    @Override
    public String createColumnFamily(CfDef colFamDef) throws Exception {
        CassandraClient client = cluster.borrowClient();
        try {
            Client thriftClient = client.getCassandra();
            thriftClient.set_keyspace(QUEUE_KEYSPACE_NAME);
            return thriftClient.system_add_column_family(colFamDef);
        }
        finally {
            cluster.releaseClient(client);
        }
    }

    @Override
    protected QueueDescriptorFactoryAbstractImpl getQueueDescriptorFactory() {
        return this.qDescFactory;
    }

    @Override
    public void shutdown() {
        // do nothing
    }
}
