package com.real.cassandra.queue.repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueDescriptorFactoryAbstractImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;

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
    private static Logger logger = LoggerFactory.getLogger(QueueRepositoryAbstractImpl.class);

    public static final String QUEUE_POOL_NAME = "myTestPool";
    public static final String SYSTEM_POOL_NAME = "mySystemPool";
    public static final String QUEUE_NAME = "myTestQueue";
    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    public static final String QUEUE_KEYSPACE_NAME = "Queues";

    public static final String QUEUE_DESCRIPTORS_COLFAM = "QueueDescriptors";
    public static final String QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE = "maxPushTimeOfPipe";
    public static final String QDESC_COLNAME_MAX_PUSHES_PER_PIPE = "maxPushesPerPipe";
    public static final String QDESC_COLNAME_MAX_POP_WIDTH = "maxPopWidth";
    public static final String QDESC_COLNAME_POP_PIPE_REFRESH_DELAY = "popPipeRefreshDelay";

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

        KsDef ksDef = createKeyspaceDefinition();
        schemaVer = createKeyspace(ksDef);

        while (!isSchemaInSync(schemaVer)) {
            logger.info("waiting for cassandra nodes to sync configuration");
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                // do nothing
            }
        }
    }

    public QueueDescriptor createQueueIfDoesntExist(String qName, long maxPushTimePerPipe, int maxPushesPerPipe,
            int maxPopWidth, long popPipeRefreshDelay) throws Exception {
        CfDef colFamDef =
                new CfDef(QUEUE_KEYSPACE_NAME, formatWaitingColFamName(qName)).setComparator_type("TimeUUIDType")
                        .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS);
        try {
            createColumnFamily(colFamDef);
        }
        catch (Exception e) {
            logger.info("exception while trying to create column family, " + colFamDef.getName()
                    + " - possibly already exists and is OK");
        }

        colFamDef =
                new CfDef(QUEUE_KEYSPACE_NAME, formatCommitPendingColFamName(qName)).setComparator_type("TimeUUIDType")
                        .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS);
        try {
            createColumnFamily(colFamDef);
        }
        catch (Exception e) {
            logger.info("exception while trying to create column family, " + colFamDef.getName()
                    + " - possibly already exists and is OK");
        }

        return createQueueDescriptorIfNotExists(qName, maxPushTimePerPipe, maxPushesPerPipe, maxPopWidth,
                popPipeRefreshDelay);
    }

    private QueueDescriptor createQueueDescriptorIfNotExists(String qName, long maxPushTimePerPipe,
            int maxPushesPerPipe, int maxPopWidth, long popPipeRefreshDelay) throws Exception {
        QueueDescriptor qDesc = getQueueDescriptor(qName);
        if (null != qDesc) {
            if (qDesc.getMaxPushesPerPipe() != maxPushesPerPipe || qDesc.getMaxPushTimeOfPipe() != maxPushTimePerPipe
                    || qDesc.getMaxPopWidth() != maxPopWidth) {
                // throw new IllegalArgumentException(
                // "Queue Descriptor already exists and you passed in parameters that do not match what is already there");
                logger.warn("Queue Descriptor already exists and you passed in parameters that do not match what is already there - using parameters from database");
            }
            return qDesc;
        }

        createQueueDescriptor(qName, maxPushTimePerPipe, maxPushesPerPipe, maxPopWidth, popPipeRefreshDelay);
        return getQueueDescriptorFactory().createInstance(qName, maxPushTimePerPipe, maxPushesPerPipe);

    }

    private KsDef createKeyspaceDefinition() throws Exception {
        ArrayList<CfDef> cfDefList = new ArrayList<CfDef>(2);
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, QUEUE_DESCRIPTORS_COLFAM).setComparator_type("BytesType")
                .setKey_cache_size(0).setRow_cache_size(1000).setGc_grace_seconds(GC_GRACE_SECS));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, PIPE_STATUS_COLFAM).setComparator_type("TimeUUIDType")
                .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS));

        return new KsDef(QUEUE_KEYSPACE_NAME, STRATEGY_CLASS_NAME, getReplicationFactor(), cfDefList);
    }

    public boolean isSchemaInSync(String version) throws Exception {
        Map<String, List<String>> schemaMap = getSchemaVersionMap();
        return null != schemaMap && 1 == schemaMap.size() && schemaMap.containsKey(version);
    }

    public static String formatWaitingColFamName(String qName) {
        return qName + WAITING_COLFAM_SUFFIX;
    }

    public static String formatCommitPendingColFamName(String qName) {
        return qName + PENDING_COMMIT_COLFAM_SUFFIX;
    }

    //
    // - override these methods
    //

    protected abstract QueueDescriptorFactoryAbstractImpl getQueueDescriptorFactory();

    protected abstract String createKeyspace(KsDef ksDef) throws Exception;

    protected abstract String dropKeyspace() throws Exception;

    public abstract QueueDescriptor getQueueDescriptor(String qName) throws Exception;

    protected abstract Map<String, List<String>> getSchemaVersionMap() throws Exception;

    protected abstract boolean isKeyspaceExists() throws Exception;

    public abstract void insert(String qName, PipeDescriptorImpl pipeDesc, UUID msgId, String msgData) throws Exception;

    public abstract void setPipeDescriptorStatus(String name, PipeDescriptorImpl pipeDesc, String statusPushFinished)
            throws Exception;

    public abstract void removeMsgFromCommitPendingPipe(CassQMsg qMsg) throws Exception;

    public abstract void removeMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc, CassQMsg qMsg) throws Exception;

    public abstract List<PipeDescriptorImpl> getOldestNonEmptyPipes(String name, int maxPopWidth) throws Exception;

    public abstract CassQMsg getOldestMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc) throws Exception;

    public abstract void moveMsgFromWaitingToCommitPendingPipe(CassQMsg qMsg) throws Exception;

    public abstract void dropQueue(CassQueueImpl cassQueueImpl) throws Exception;

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

    public abstract void createColumnFamily(CfDef colFamDef) throws Exception;

    protected abstract void createQueueDescriptor(String qName, long maxPushTimePerPipe, int maxPushesPerPipe,
            int maxPopWidth, long popPipeRefreshDelay) throws Exception;

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

}
