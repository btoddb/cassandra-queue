package com.real.cassandra.queue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.InstanceAlreadyExistsException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.utils.UUIDGen;
import org.scale7.cassandra.pelops.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.repository.QueueRepository;

/**
 * Implementation of a simple FIFO queue using Cassandra as persistent storage.
 * No caching or priorities are implemented to keep it easy and simple.
 * 
 * <p/>
 * Uses multiple keys (rows) per queue (I call them "pipes") to help distribute
 * data across the cluster.
 * <p/>
 * {@link #push(String)} will push a value onto one of the pipes in the queue.
 * The pipe is chosen in round robin fashion. No locking is necessary as pushing
 * messages is inherently safe.
 * <p/>
 * {@link #pop()} will either read from all pipes and return the oldest message
 * or simply read from one pipe, depending on whether "Near FIFO" mode is on.
 * <p/>
 * "Near FIFO" is the default operating behavior. It means that a pop will
 * retrieve the oldest message from one of the pipes, but it may not be the
 * oldest message in the queue. (Round robin is used to choose the pipe.) If
 * strict FIFO is required, use {@link #setNearFifoOk(boolean)} to set it to
 * false. This will cause a degradation in performance because all pipes must be
 * read to determine the oldest message. multiget is used to reduce the wire
 * time and increase parallelism in Cassandra.
 * 
 * @author Todd Burruss
 */
public class CassQueue implements CassQueueMXBean {
    private static Logger logger = LoggerFactory.getLogger(CassQueue.class);

    private InetAddress inetAddr;

    private EnvProperties envProps;
    private QueueRepository qRepos;
    private String name;
    private boolean nearFifoOk = true;
    private PopLock popLock;

    // pipe mgmt
    private PipeManager pipeMgr;
    private PipeSelectionRoundRobinStrategy pipeSelector;

    // stats
    private AtomicLong msgCountCurrent = new AtomicLong();
    private AtomicLong pushCountTotal = new AtomicLong();
    private AtomicLong popCountTotal = new AtomicLong();
    private AtomicLong commitTotal = new AtomicLong();
    private AtomicLong rollbackTotal = new AtomicLong();

    private RollingStat pushTimes = new RollingStat(30000);
    private RollingStat popTimes = new RollingStat(30000);
    private RollingStat popLockWaitTimes = new RollingStat(30000);
    private RollingStat commitTimes = new RollingStat(30000);
    private RollingStat rollbackTimes = new RollingStat(30000);
    private RollingStat moveToDeliveredTimes = new RollingStat(30000);
    private RollingStat getWaitingMsgTimes = new RollingStat(30000);
    private RollingStat getNextPopPipeTimes = new RollingStat(30000);
    private RollingStat getNextPushPipeTimes = new RollingStat(30000);

    /**
     * 
     * @param qRepos
     *            Repository used to communicate to/from Cassandra cluster
     * @param name
     *            Name of the Queue
     * @param numPipes
     *            The number of "rows" the queue uses internally to help
     *            distibute data across cluster.
     * @param popLocks
     *            Client desires <code>pop</code>s to be locked so as to allow
     *            only one thread in pop routine at a time. Locking mechanism
     *            depends on the value of <code>distributed</code>.
     * @param distributed
     *            Whether or not utilize cross JVM locking capability (Uses
     *            ZooKeeper.)
     * @throws Exception
     */
    public CassQueue(QueueRepository queueRepository, String name, boolean popLocks, boolean distributed,
            EnvProperties envProps) throws Exception {
        this.envProps = envProps;
        if (null == envProps) {
            throw new IllegalArgumentException("queue environment properties cannot be null");
        }

        if (null == name) {
            throw new IllegalArgumentException("queue name cannot be null");
        }

        if (0 >= envProps.getNumPipes()) {
            throw new IllegalArgumentException("queue must be setup with one or more pipes");
        }

        this.qRepos = queueRepository;
        this.name = name;

        initJmx();
        initUuidCreator();
        initPipeManager();
        initPipeSelectionStrategy();

        if (popLocks) {
            if (distributed) {
                popLock = new PopLockDistributedImpl(this.name, envProps.getNumPipes());
            }
            else {
                popLock = new PopLockLocalImpl(envProps.getNumPipes());
            }
        }
        else {
            popLock = new PopLockNoOpImpl();
        }

        initQueue();
    }

    private void initPipeManager() {
        pipeMgr = new PipeManager(getName());
    }

    private void initPipeSelectionStrategy() {
        pipeSelector = new PipeSelectionRoundRobinStrategy(envProps, getName(), pipeMgr, qRepos);
    }

    private void initQueue() throws Exception {
        QueueDescriptor qDesc = qRepos.getQueueDescriptor(name);

        long startPipe = qDesc.getPopStartPipe() - 1;
        startPipe = 0 <= startPipe ? startPipe : 0;
        long endPipe = qDesc.getPushStartPipe() + envProps.getNumPipes();

        for (Long pipeNum = startPipe; pipeNum <= endPipe; pipeNum++) {
            pipeMgr.addPipe(pipeNum);
        }

        int count = qRepos.getCount(getName(), startPipe, endPipe);
        this.msgCountCurrent.set(count);
    }

    public void shutdown() {
        pipeSelector.shutdown();
        pipeMgr.shutdown();
    }

    /**
     * Push a value onto the Queue. Choose in round robin fashion the next pipe.
     * 
     * @param value
     *            String value to persist in queue
     * @throws Exception
     */
    public void push(String value) throws Exception {
        long pipeNum;
        long start = System.currentTimeMillis();

        pipeNum = pipeSelector.pickPushPipe();
        getNextPushPipeTimes.addSample(System.currentTimeMillis() - start);

        try {
            UUID timeUuid = UUIDGen.makeType1UUIDFromHost(inetAddr);
            qRepos.insert(QueueRepository.WAITING_COL_FAM, getName(), pipeNum, Bytes.fromUuid(timeUuid),
                    Bytes.fromUTF8(value));
            pushCountTotal.incrementAndGet();
            msgCountCurrent.incrementAndGet();
            pipeMgr.incPushCount(pipeNum);
            pushTimes.addSample(System.currentTimeMillis() - start);
        }
        finally {
            pipeSelector.releasePushPipe(pipeNum);
        }
    }

    // private void decPipeCountCurrent(Long pipeNum) {
    // pipeCountCurrent.get(pipeNum).decrementAndGet();
    // }

    /**
     * Pop the oldest message from the queue. If no messages, null is returned.
     * 
     * @return Oldest message in the queue, or null if no messages.
     * @throws Exception
     */
    public CassQMsg pop() throws Exception {
        long start = System.currentTimeMillis();

        CassQMsg qMsg;
        if (nearFifoOk) {
            qMsg = nearFifoPop();
        }
        else {
            qMsg = strictFifoPop();
        }

        if (null != qMsg) {
            popCountTotal.incrementAndGet();
            msgCountCurrent.decrementAndGet();
            popTimes.addSample(System.currentTimeMillis() - start);
        }
        return qMsg;
    }

    private CassQMsg nearFifoPop() throws Exception {
        PipeDescriptor pipeDesc = null;
        Column col = null;

        // try to get a pipe with data "numPipes" times
        for (int i = 0; i < envProps.getNumPipes(); i++) {
            PipeDescriptor tmpDesc = lockNextFreePopPipe();
            try {
                long startPopLockWait = System.currentTimeMillis();
                synchronized (pipeMgr.getPipeMonitor(tmpDesc)) {
                    popLockWaitTimes.addSample(System.currentTimeMillis() - startPopLockWait);
                    long start = System.currentTimeMillis();
                    List<Column> colList = qRepos.getWaitingMessages(getName(), tmpDesc.getPipeNum(), 1);
                    getWaitingMsgTimes.addSample(System.currentTimeMillis() - start);
                    if (!colList.isEmpty()) {
                        logger.debug("pipe has data : " + pipeDesc);
                        pipeDesc = tmpDesc;
                        col = colList.get(0);
                        moveToDelivered(pipeDesc, col);
                        break;
                    }
                    else {
                        pipeSelector.popPipeEmpty(tmpDesc);
                    }
                }
            }
            finally {
                pipeSelector.releasePopPipe(tmpDesc.getPipeNum());
            }
        }

        // if no "oldest", then return null
        if (null == pipeDesc) {
            return null;
        }

        UUID colName = UUIDGen.makeType1UUID(col.getName());
        return new CassQMsg(pipeDesc, colName, new String(col.getValue()));
    }

    private CassQMsg strictFifoPop() throws Exception {
        PipeDescriptor pipeDesc = null;
        Column col = null;
        long start = System.currentTimeMillis();
        Map<Bytes, List<Column>> colList = qRepos.getOldestFromAllPipes(pipeMgr.getPopAllKeyList());
        getWaitingMsgTimes.addSample(System.currentTimeMillis() - start);

        UUID oldestColName = null;
        // determine which result is the oldest across the queue rows
        for (Entry<Bytes, List<Column>> entry : colList.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }

            Column tmpCol = entry.getValue().get(0);
            UUID colName = UUIDGen.makeType1UUID(tmpCol.getName());
            if (null == pipeDesc || -1 == colName.compareTo(oldestColName)) {
                pipeDesc = pipeMgr.getPipeDescriptor(entry.getKey());
                col = tmpCol;
                oldestColName = colName;
            }
        }

        // if no "oldest", then return null
        if (null == pipeDesc) {
            return null;
        }

        moveToDelivered(pipeDesc, col);

        UUID colName = UUIDGen.makeType1UUID(col.getName());
        return new CassQMsg(pipeDesc, colName, new String(col.getValue()));
    }

    private void moveToDelivered(PipeDescriptor pipeDesc, Column col) throws Exception {
        // decPipeCountCurrent(pipeDesc.getPipeNum());

        long moveStart = System.currentTimeMillis();
        qRepos.moveFromWaitingToDelivered(pipeDesc, Bytes.fromBytes(col.getName()), Bytes.fromBytes(col.getValue()));
        moveToDeliveredTimes.addSample(System.currentTimeMillis() - moveStart);
    }

    /**
     * Commit the 'pop' of a previous message. If commit is not called, the
     * message will be rolled back ({@link #rollback(CassQMsg)}).
     * 
     * @param qMsg
     * @throws Exception
     */
    public void commit(CassQMsg qMsg) throws Exception {
        long start = System.currentTimeMillis();
        qRepos.removeFromDelivered(qMsg.getQueuePipeDescriptor(), Bytes.fromUuid(qMsg.getMsgId()));
        commitTotal.incrementAndGet();
        commitTimes.addSample(System.currentTimeMillis() - start);
    }

    /**
     * Rollback a message. This means the message will be available again and
     * eventually pop'ed from the queue.
     * 
     * @param qMsg
     * @throws Exception
     */
    public void rollback(CassQMsg qMsg) throws Exception {
        long start = System.currentTimeMillis();
        qRepos.moveFromDeliveredToWaiting(qMsg.getQueuePipeDescriptor(), Bytes.fromUuid(qMsg.getMsgId()),
                Bytes.fromUTF8(qMsg.getValue()));
        msgCountCurrent.incrementAndGet();
        popCountTotal.decrementAndGet();
        rollbackTotal.incrementAndGet();
        rollbackTimes.addSample(System.currentTimeMillis() - start);
    }

    /**
     * Remove all data from the queue. This includes all waiting and uncommitted
     * data. In addition, the next pipe to use is reset.
     * 
     * @throws Exception
     */
    public void truncate() throws Exception {
        // enter ZK lock region

        qRepos.truncateQueueData(this);
        qRepos.createQueue(getName(), envProps.getNumPipes());

        // release ZK lock region
    }

    private PipeDescriptor lockNextFreePopPipe() throws Exception {
        long start = System.currentTimeMillis();

        try {
            return pipeSelector.pickPopPipe();
        }
        finally {
            getNextPopPipeTimes.addSample(System.currentTimeMillis() - start);
        }
    }

    private void initJmx() {
        String beanName = JMX_MBEAN_OBJ_NAME + "-" + name;
        try {
            JmxMBeanManager.getInstance().registerMBean(this, beanName);
        }
        catch (InstanceAlreadyExistsException e1) {
            logger.warn("exception while registering MBean, " + beanName + " - ignoring");
        }
        catch (Exception e) {
            throw new RuntimeException("exception while registering MBean, " + beanName);
        }
    }

    private void initUuidCreator() {
        try {
            inetAddr = InetAddress.getLocalHost();
        }
        catch (UnknownHostException e) {
            logger.error("exception while getting local IP address", e);
            throw new RuntimeException(e);
        }
    }

    public void setStopPipeWatcher() {
        pipeSelector.shutdown();
    }

    /**
     * Get up to <code>maxMessages</code> messsages waiting to be pop'ed.
     * Returns a list of raw Cassandra <code>Column</code>s as this method is
     * intended for testing only.
     * 
     * @param pipeNum
     * @param maxMessags
     * @return List of raw Cassandra <code>Column</code>s, empty List if no
     *         messages
     * @throws Exception
     */
    public List<Column> getWaitingMessages(long pipeNum, int maxMessags) throws Exception {
        return qRepos.getWaitingMessages(getName(), pipeNum, maxMessags);
    }

    /**
     * Get up to <code>maxMessages</code> messsages delivered and waiting to be
     * committed. Returns a list of raw Cassandra <code>Column</code>s as this
     * method is intended for testing only.
     * 
     * @param pipeNum
     * @param maxMessags
     * @return List of raw Cassandra <code>Column</code>s, empty List if no
     *         messages
     * @throws Exception
     */
    public List<Column> getDeliveredMessages(long pipeNum, int maxMessages) throws Exception {
        return qRepos.getDeliveredMessages(getName(), pipeNum, maxMessages);
    }

    /**
     * Return status of near FIFO.
     * 
     * @return
     */
    @Override
    public boolean isNearFifoOk() {
        return nearFifoOk;
    }

    /**
     * Set near FIFO.
     * 
     * @param nearFifoOk
     */
    public void setNearFifoOk(boolean nearFifoOk) {
        this.nearFifoOk = nearFifoOk;
    }

    /**
     * Return name of queue.
     * 
     * @return
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Return number of pipes.
     * 
     * @return
     */
    @Override
    public int getNumPipes() {
        return envProps.getNumPipes();
    }

    @Override
    public long getMsgCountCurrent() {
        return msgCountCurrent.get();
    }

    @Override
    public long getPushCountTotal() {
        return pushCountTotal.get();
    }

    @Override
    public long getPopCountTotal() {
        return popCountTotal.get();
    }

    @Override
    public long getCommitTotal() {
        return commitTotal.get();
    }

    @Override
    public long getRollbackTotal() {
        return rollbackTotal.get();
    }

    @Override
    public double getPushAvgTime() {
        return pushTimes.getAvgOfValues();
    }

    @Override
    public double getPushesPerSecond() {
        return pushTimes.getSamplesPerSecond();
    }

    @Override
    public double getPopAvgTime() {
        return popTimes.getAvgOfValues();
    }

    @Override
    public double getPopsPerSecond() {
        return popTimes.getSamplesPerSecond();
    }

    @Override
    public double getCommitAvgTime() {
        return commitTimes.getAvgOfValues();
    }

    @Override
    public double getCommitsPerSecond() {
        return commitTimes.getSamplesPerSecond();
    }

    @Override
    public double getRollbackAvgTime() {
        return rollbackTimes.getAvgOfValues();
    }

    @Override
    public double getRollbacksPerSecond() {
        return rollbackTimes.getSamplesPerSecond();
    }

    @Override
    public String[] getPipeCounts() {
        return pipeMgr.getPipeCounts();
    }

    @Override
    public double getMoveToDeliveredAvgTime() {
        return moveToDeliveredTimes.getAvgOfValues();
    }

    @Override
    public double getWaitingMsgAvgTime() {
        return getWaitingMsgTimes.getAvgOfValues();
    }

    public double getPopLockWaitAvgTime() {
        return popLockWaitTimes.getAvgOfValues();
    }

    public long getPopStartPipe() {
        return pipeSelector.getPopStartPipe();
    }

    public long getPushStartPipe() {
        return pipeSelector.getPushStartPipe();
    }

    @Override
    public double getGetNextPopPipeAvgTime() {
        return getNextPopPipeTimes.getAvgOfValues();
    }

    @Override
    public double getGetNextPushPipeAvgTime() {
        return getNextPushPipeTimes.getAvgOfValues();
    }
}
