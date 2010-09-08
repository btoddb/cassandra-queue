package com.real.cassandra.queue.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.scale7.cassandra.pelops.Bytes;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.repository.pelops.QueueRepositoryImpl.CountResult;

/**
 * Responsible for the raw I/O for Cassandra queues. Uses Pelops library for
 * client communication to Cassandra server.
 * 
 * <p/>
 * Requires Cassandra 0.7 or better.
 * 
 * @author Todd Burruss
 */
public abstract class QueueRepositoryAbstractImpl {
    public static final String QUEUE_KEYSPACE_NAME = "Queues";

    public static final String QUEUE_DESCRIPTORS_COLFAM = "QueueDescriptors";
    public static final Bytes QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE = Bytes.fromUTF8("maxPushTimeOfPipe");
    public static final Bytes QDESC_COLNAME_MAX_PUSHES_PER_PIPE = Bytes.fromUTF8("maxPushesPerPipe");
    public static final Bytes QDESC_COLNAME_MAX_POP_WIDTH = Bytes.fromUTF8("maxPopWidth");
    public static final Bytes QDESC_COLNAME_POP_PIPE_REFRESH_DELAY = Bytes.fromUTF8("popPipeRefreshDelay");

    public static final String PIPE_STATUS_COLFAM = "PipeStatus";

    protected static final String WAITING_COLFAM_SUFFIX = "_Waiting";
    protected static final String PENDING_COMMIT_COLFAM_SUFFIX = "_PendingCommit";
    protected static final int GC_GRACE_SECS = 86400; // one day

    protected static final int MAX_QUEUE_DESCRIPTOR_COLUMNS = 100;

    public static final String SYSTEM_KEYSPACE_NAME = "system";
    public static final String STRATEGY_CLASS_NAME = "org.apache.cassandra.locator.SimpleStrategy";

    private final int replicationFactor;
    private final ConsistencyLevel consistencyLevel;

    public QueueRepositoryAbstractImpl(int replicationFactor, ConsistencyLevel consistencyLevel) {
        this.replicationFactor = replicationFactor;
        this.consistencyLevel = consistencyLevel;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public boolean isQueueExists(String qName) throws Exception {
        return null != getQueueDescriptor(qName);
    }

    /**
     * Initialize cassandra server for use with queues.
     * 
     * @param forceRecreate
     *            if true will drop the keyspace and recreate it.
     * @throws Exception
     */
    public void initKeyspace(boolean forceRecreate) throws Exception {
        String schemaVer = null;
        if (isKeyspaceExists()) {
            if (!forceRecreate) {
                return;
            }
            else {
                schemaVer = dropKeyspace();
            }
        }

        Thread.sleep(2000);

        KsDef ksDef = createKeyspaceDefinition();
        schemaVer = createKeyspace(ksDef);

        String version = getSchemaVersion();

        // give the cluster a chance to propagate keyspaces created in previous
        Thread.sleep(2000);
    }

    private KsDef createKeyspaceDefinition() throws Exception {
        ArrayList<CfDef> cfDefList = new ArrayList<CfDef>(2);
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, QUEUE_DESCRIPTORS_COLFAM).setComparator_type("BytesType")
                .setKey_cache_size(0).setRow_cache_size(1000).setGc_grace_seconds(GC_GRACE_SECS));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, PIPE_STATUS_COLFAM).setComparator_type("TimeUUIDType")
                .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS));

        return new KsDef(QUEUE_KEYSPACE_NAME, STRATEGY_CLASS_NAME, getReplicationFactor(), cfDefList);
    }

    //
    // - override these methods
    //

    protected abstract String createKeyspace(KsDef ksDef) throws Exception;

    protected abstract String dropKeyspace() throws Exception;

    public abstract QueueDescriptor getQueueDescriptor(String qName) throws Exception;

    protected abstract String getSchemaVersion() throws Exception;

    protected abstract boolean isKeyspaceExists() throws Exception;

    public abstract void insert(String qName, PipeDescriptorImpl pipeDesc, UUID msgId, String msgData) throws Exception;

    public abstract void setPipeDescriptorStatus(String name, PipeDescriptorImpl pipeDesc, String statusPushFinished)
            throws Exception;

    public abstract void removeMsgFromDeliveredPipe(CassQMsg qMsg) throws Exception;

    public abstract void removeMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc, CassQMsg qMsg) throws Exception;

    public abstract List<PipeDescriptorImpl> getOldestNonEmptyPipes(String name, int maxPopWidth) throws Exception;

    public abstract CassQMsg getOldestMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc) throws Exception;

    public abstract void moveMsgFromWaitingToDeliveredPipe(CassQMsg qMsg) throws Exception;

    public abstract QueueDescriptor createQueueIfDoesntExist(String qName, long maxPushTimeOfPipe,
            int maxPushesPerPipe, int maxPopWidth, long popPipeRefreshDelay) throws Exception;

    public abstract void truncateQueueData(CassQueueImpl cassQueueImpl) throws Exception;

    public abstract CassQMsg getMsg(String qName, UUID pipeId, UUID msgId) throws Exception;

    public abstract void createPipeDescriptor(String qName, UUID pipeId, String pipeStatus) throws Exception;

    public abstract PipeDescriptorImpl getPipeDescriptor(String qName, UUID pipeId) throws Exception;

    public abstract CountResult getCountOfWaitingMsgs(String qName) throws Exception;

    public abstract CountResult getCountOfPendingCommitMsgs(String qName) throws Exception;

    public abstract List<CassQMsg> getDeliveredMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxMsgs)
            throws Exception;

    public abstract List<CassQMsg> getWaitingMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxColumns)
            throws Exception;

    public abstract CassQMsg getOldestMsgFromDeliveredPipe(PipeDescriptorImpl pipeDesc) throws Exception;

    public abstract KsDef getKeyspaceDefinition() throws Exception;
}
