package com.real.cassandra.queue.repository.hector;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.model.ColumnSlice;
import me.prettyprint.cassandra.model.KeyspaceOperator;
import me.prettyprint.cassandra.model.Mutator;
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.model.Result;
import me.prettyprint.cassandra.model.SliceQuery;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueDescriptorFactoryAbstractImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;

public class QueueRepositoryImpl extends QueueRepositoryAbstractImpl {
    private Cluster cluster;
    private QuorumAllConsistencyLevelPolicy consistencyLevelPolicy = new QuorumAllConsistencyLevelPolicy();
    private QueueDescriptorFactoryImpl qDescFactory;

    public QueueRepositoryImpl(Cluster cluster, int replicationFactor, ConsistencyLevel consistencyLevel) {
        super(replicationFactor, consistencyLevel);
        this.cluster = cluster;
        this.qDescFactory = new QueueDescriptorFactoryImpl();
    }

    @Override
    public QueueDescriptor getQueueDescriptor(String qName) throws Exception {
        StringSerializer se = StringSerializer.get();
        IntegerSerializer ie = IntegerSerializer.get();
        LongSerializer le = LongSerializer.get();

        KeyspaceOperator ko = createKeyspaceOperator();
        SliceQuery<String, String, String> q = HFactory.createSliceQuery(ko, se, se, se);
        q.setRange(null, null, false, 100);
        q.setColumnFamily(QUEUE_DESCRIPTORS_COLFAM);
        q.setKey(qName);
        Result<ColumnSlice<String, String>> r = q.execute();
        return qDescFactory.createInstance(qName, r);
    }

    @Override
    protected void createQueueDescriptor(String qName, long maxPushTimePerPipe, int maxPushesPerPipe, int maxPopWidth,
            long popPipeRefreshDelay) throws Exception {
        KeyspaceOperator ko = createKeyspaceOperator();

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

    private KeyspaceOperator createKeyspaceOperator() {
        return HFactory.createKeyspaceOperator(QUEUE_KEYSPACE_NAME, cluster, consistencyLevelPolicy);
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
            schemaMap = thriftClient.check_schema_agreement();
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
    public void insert(String qName, PipeDescriptorImpl pipeDesc, UUID msgId, String msgData) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void setPipeDescriptorStatus(String name, PipeDescriptorImpl pipeDesc, String statusPushFinished)
            throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeMsgFromCommitPendingPipe(CassQMsg qMsg) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc, CassQMsg qMsg) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public List<PipeDescriptorImpl> getOldestNonEmptyPipes(String name, int maxPopWidth) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CassQMsg getOldestMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void moveMsgFromWaitingToCommitPendingPipe(CassQMsg qMsg) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void truncateQueueData(CassQueueImpl cassQueueImpl) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public CassQMsg getMsg(String qName, UUID pipeId, UUID msgId) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void createPipeDescriptor(String qName, UUID pipeId, String pipeStatus) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public PipeDescriptorImpl getPipeDescriptor(String qName, UUID pipeId) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CountResult getCountOfWaitingMsgs(String qName) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CountResult getCountOfPendingCommitMsgs(String qName) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<CassQMsg> getDeliveredMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxMsgs) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<CassQMsg> getWaitingMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxColumns) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CassQMsg getOldestMsgFromDeliveredPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public KsDef getKeyspaceDefinition() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void createColumnFamily(CfDef colFamDef) throws Exception {
        CassandraClient client = cluster.borrowClient();
        try {
            Client thriftClient = client.getCassandra();
            thriftClient.system_add_column_family(colFamDef);
        }
        finally {
            cluster.releaseClient(client);
        }
    }

    @Override
    protected QueueDescriptorFactoryAbstractImpl getQueueDescriptorFactory() {
        return this.qDescFactory;
    }
}
