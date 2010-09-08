package com.real.cassandra.queue.repository.hector;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
import com.real.cassandra.queue.repository.pelops.QueueRepositoryImpl.CountResult;

public class QueueRepositoryImpl extends QueueRepositoryAbstractImpl {
    private Cluster cluster;

    public QueueRepositoryImpl(int replicationFactor, ConsistencyLevel consistencyLevel) {
        super(replicationFactor, consistencyLevel);
    }

    @Override
    public QueueDescriptor getQueueDescriptor(String qName) throws Exception {
        // TODO Auto-generated method stub
        return null;
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
    protected String getSchemaVersion() throws Exception {
        Map<String, List<String>> schemaMap;
        CassandraClient client = cluster.borrowClient();
        try {
            Client thriftClient = client.getCassandra();
            schemaMap = thriftClient.check_schema_agreement();
        }
        finally {
            cluster.releaseClient(client);
        }

        return schemaMap.get(QUEUE_KEYSPACE_NAME).get(0);
    }

    public void initConnectionPool() {
        cluster = HFactory.getOrCreateCluster("CassQueue", "localhost");
    }

    @Override
    protected String dropKeyspace() throws Exception {
        // TODO Auto-generated method stub
        return null;
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
    public void removeMsgFromDeliveredPipe(CassQMsg qMsg) throws Exception {
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
    public void moveMsgFromWaitingToDeliveredPipe(CassQMsg qMsg) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public QueueDescriptor createQueueIfDoesntExist(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe,
            int maxPopWidth, long popPipeRefreshDelay) throws Exception {
        // TODO Auto-generated method stub
        return null;
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
}
