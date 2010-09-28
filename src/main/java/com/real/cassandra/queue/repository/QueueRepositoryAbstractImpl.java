package com.real.cassandra.queue.repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQMsgFactory;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueDescriptorFactoryAbstractImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorFactory;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatusFactory;
import com.real.cassandra.queue.repository.hector.QueueDescriptorFactoryImpl;

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

    public static final String QUEUE_POOL_NAME = "queuePool";
    public static final String SYSTEM_POOL_NAME = "mySystemPool";
    public static final String QUEUE_NAME = "myTestQueue";

    public static final String QUEUE_KEYSPACE_NAME = "Queues";

    public static final String QUEUE_DESCRIPTORS_COLFAM = "QueueDescriptors";
    public static final String QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE = "maxPushTimeOfPipe";
    public static final String QDESC_COLNAME_MAX_PUSHES_PER_PIPE = "maxPushesPerPipe";
    public static final String QDESC_COLNAME_MAX_POP_WIDTH = "maxPopWidth";
    public static final String QDESC_COLNAME_POP_PIPE_REFRESH_DELAY = "popPipeRefreshDelay";

    public static final String PIPE_STATUS_COLFAM = "PipeStatus";

    protected static final String WAITING_COLFAM_SUFFIX = "_Waiting";
    protected static final String PENDING_COLFAM_SUFFIX = "_Pending";
    protected static final int GC_GRACE_SECS = 86400; // one day

    protected static final int MAX_QUEUE_DESCRIPTOR_COLUMNS = 100;

    public static final String SYSTEM_KEYSPACE_NAME = "system";
    public static final String STRATEGY_CLASS_NAME = "org.apache.cassandra.locator.SimpleStrategy";

    private static final long MAX_WAIT_SCHEMA_SYNC = 10000;

    protected PipeDescriptorFactory pipeDescFactory;
    protected QueueDescriptorFactoryImpl qDescFactory;
    protected PipeStatusFactory pipeStatusFactory = new PipeStatusFactory();
    protected CassQMsgFactory qMsgFactory = new CassQMsgFactory();

    private final int replicationFactor;

    public QueueRepositoryAbstractImpl(int replicationFactor) {
        this.replicationFactor = replicationFactor;
        this.pipeDescFactory = new PipeDescriptorFactory(this);
        this.qDescFactory = new QueueDescriptorFactoryImpl();

    }

    /**
     * Perform default initialization of the repository. Intended use is for
     * spring 'init-method'
     * 
     * @throws Exception
     */
    public void init() throws Exception {
        initKeyspace(false);
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
    public void initKeyspace(boolean forceRecreate) {
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

    /**
     * Creates the queue descriptor if doesn't exists, otherwise uses values
     * from database disregarding the parameters passed from client. Issues
     * warning if parameters don't match database.
     * 
     * @param qName
     * @param maxPushTimePerPipe
     * @param maxPushesPerPipe
     * @param maxPopWidth
     * @param popPipeRefreshDelay
     * @return
     * @throws Exception
     */
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
                new CfDef(QUEUE_KEYSPACE_NAME, formatPendingColFamName(qName)).setComparator_type("TimeUUIDType")
                        .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS);
        String ver = null;
        try {
            ver = createColumnFamily(colFamDef);
        }
        catch (Exception e) {
            logger.info("exception while trying to create column family, " + colFamDef.getName()
                    + " - possibly already exists and is OK");
        }

        waitForSchemaSync(ver);

        return createQueueDescriptorIfNotExists(qName, maxPushTimePerPipe, maxPushesPerPipe, maxPopWidth,
                popPipeRefreshDelay);
    }

    private void waitForSchemaSync(String newVer) throws Exception {
        long start = System.currentTimeMillis();

        while (null != newVer && !isSchemaInSync(newVer) && (System.currentTimeMillis() - start < MAX_WAIT_SCHEMA_SYNC)) {
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }

    private QueueDescriptor createQueueDescriptorIfNotExists(String qName, long maxPushTimePerPipe,
            int maxPushesPerPipe, int maxPopWidth, long popPipeRefreshDelay) throws Exception {
        QueueDescriptor qDesc = getQueueDescriptor(qName);
        if (null != qDesc) {
            if (qDesc.getMaxPushesPerPipe() != maxPushesPerPipe || qDesc.getMaxPushTimeOfPipe() != maxPushTimePerPipe
                    || qDesc.getMaxPopWidth() != maxPopWidth) {
                logger.warn("Queue Descriptor already exists and you passed in parameters that do not match what is already there - using parameters from database");
            }
            return qDesc;
        }

        createQueueDescriptor(qName, maxPushTimePerPipe, maxPushesPerPipe, maxPopWidth, popPipeRefreshDelay);
        return getQueueDescriptorFactory().createInstance(qName, maxPushTimePerPipe, maxPushesPerPipe);

    }

    private KsDef createKeyspaceDefinition() {
        ArrayList<CfDef> cfDefList = new ArrayList<CfDef>(2);
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, QUEUE_DESCRIPTORS_COLFAM).setComparator_type("BytesType")
                .setKey_cache_size(0).setRow_cache_size(1000).setGc_grace_seconds(GC_GRACE_SECS));
        cfDefList.add(new CfDef(QUEUE_KEYSPACE_NAME, PIPE_STATUS_COLFAM).setComparator_type("TimeUUIDType")
                .setKey_cache_size(0).setRow_cache_size(0).setGc_grace_seconds(GC_GRACE_SECS));

        return new KsDef(QUEUE_KEYSPACE_NAME, STRATEGY_CLASS_NAME, getReplicationFactor(), cfDefList);
    }

    public boolean isSchemaInSync(String version) {
        Map<String, List<String>> schemaMap = getSchemaVersionMap();
        return null != schemaMap && 1 == schemaMap.size() && schemaMap.containsKey(version);
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

    public CountResult getCountOfPendingCommitMsgs(String qName, int maxMsgCount) throws Exception {
        return getCountOfMsgsAndStatus(qName, formatPendingColFamName(qName), maxMsgCount);
    }

    public CassQMsg getOldestMsgFromDeliveredPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        return getOldestMsgFromPipe(formatPendingColFamName(pipeDesc.getQName()), pipeDesc);
    }

    public CassQMsg getOldestMsgFromWaitingPipe(PipeDescriptorImpl pipeDesc) throws Exception {
        return getOldestMsgFromPipe(formatWaitingColFamName(pipeDesc.getQName()), pipeDesc);
    }

    private CassQMsg getOldestMsgFromPipe(String colFamName, PipeDescriptorImpl pipeDesc) throws Exception {
        List<CassQMsg> msgList = getOldestMsgsFromPipe(colFamName, pipeDesc, 1);
        if (null != msgList && !msgList.isEmpty()) {
            return msgList.get(0);
        }
        else {
            return null;
        }
    }

    public List<CassQMsg> getDeliveredMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxMsgs) throws Exception {
        return getOldestMsgsFromPipe(formatPendingColFamName(pipeDesc.getQName()), pipeDesc, maxMsgs);
    }

    public List<CassQMsg> getWaitingMessagesFromPipe(PipeDescriptorImpl pipeDesc, int maxMsgs) throws Exception {
        return getOldestMsgsFromPipe(formatWaitingColFamName(pipeDesc.getQName()), pipeDesc, maxMsgs);
    }

    public void removeMsgFromPendingPipe(CassQMsg qMsg) throws Exception {
        PipeDescriptorImpl pipeDesc = qMsg.getPipeDescriptor();
        removeMsgFromPipe(formatPendingColFamName(pipeDesc.getQName()), pipeDesc, qMsg);
    }

    public void removeMsgFromWaitingPipe(CassQMsg qMsg) throws Exception {
        PipeDescriptorImpl pipeDesc = qMsg.getPipeDescriptor();
        removeMsgFromPipe(formatWaitingColFamName(pipeDesc.getQName()), pipeDesc, qMsg);
    }

    //
    // - override these methods
    //

    public abstract void shutdown();

    protected abstract CountResult getCountOfMsgsAndStatus(String qName, final String colFamName, int maxMsgCount);

    protected abstract QueueDescriptorFactoryAbstractImpl getQueueDescriptorFactory();

    protected abstract String createKeyspace(KsDef ksDef);

    protected abstract String dropKeyspace();

    public abstract QueueDescriptor getQueueDescriptor(String qName) throws Exception;

    protected abstract Map<String, List<String>> getSchemaVersionMap();

    protected abstract boolean isKeyspaceExists();

    public abstract void insert(PipeDescriptorImpl pipeDesc, UUID msgId, String msgData) throws Exception;

    public abstract void setPipeDescriptorStatus(PipeDescriptorImpl pipeDesc, String pipeStatus) throws Exception;

    public abstract List<PipeDescriptorImpl> getOldestNonEmptyPipes(String name, int maxNumPipeDescs) throws Exception;

    public abstract void moveMsgFromWaitingToPendingPipe(CassQMsg qMsg) throws Exception;

    public abstract void dropQueue(CassQueueImpl cassQueueImpl) throws Exception;

    public abstract void truncateQueueData(CassQueueImpl cq) throws Exception;

    public abstract CassQMsg getMsg(String qName, PipeDescriptorImpl pipeDesc, UUID msgId) throws Exception;

    public abstract void createPipeDescriptor(String qName, UUID pipeId, String pipeStatus, long createTimestamp)
            throws Exception;

    public abstract PipeDescriptorImpl getPipeDescriptor(String qName, UUID pipeId) throws Exception;

    protected abstract List<CassQMsg> getOldestMsgsFromPipe(String colFameName, PipeDescriptorImpl pipeDesc, int maxMsgs)
            throws Exception;

    public abstract KsDef getKeyspaceDefinition() throws Exception;

    public abstract String createColumnFamily(CfDef colFamDef) throws Exception;

    protected abstract void createQueueDescriptor(String qName, long maxPushTimePerPipe, int maxPushesPerPipe,
            int maxPopWidth, long popPipeRefreshDelay) throws Exception;

    protected abstract void removeMsgFromPipe(String colFamName, PipeDescriptorImpl pipeDesc, CassQMsg qMsg)
            throws Exception;

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

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("CountResult [numPipeDescriptors=");
            builder.append(numPipeDescriptors);
            builder.append(", totalMsgCount=");
            builder.append(totalMsgCount);
            builder.append(", statusCounts=");
            builder.append(statusCounts);
            builder.append("]");
            return builder.toString();
        }

    }

}
