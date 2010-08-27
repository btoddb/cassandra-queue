package com.real.cassandra.queue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final Map<Long, Object> pipeMonitorObjMap = new HashMap<Long, Object>();

    private EnvProperties envProps;
    private QueueRepository queueRepository;
    private String name;
    private Map<Bytes, PipeDescriptor> pipeDescLookupMap = new HashMap<Bytes, PipeDescriptor>();
    private List<Bytes> popAllKeyList = new LinkedList<Bytes>();
    private boolean nearFifoOk = true;
    private PopLock popLock;
    private QueueDescriptor qDesc;
    private Map<Long, AtomicLong> pipeCountCurrent = new HashMap<Long, AtomicLong>();
    private PipeWatcher pipeWatcher;

    // pipe mgmt
    private Object pushPipeIncMonitor = new Object();
    private Object popPipeIncMonitor = new Object();
    private Object pipeManipulateMonitorObj = new Object();
    private long nextPopPipeOffset = 0;
    private long nextPushPipeToUse = 0;
    private Map<Long, AtomicBoolean> pipeUseLockMap = new HashMap<Long, AtomicBoolean>();

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
     * @param queueRepository
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

        this.queueRepository = queueRepository;
        this.name = name;

        initJmx();
        initUuidCreator();

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

    private void initQueue() throws Exception {
        qDesc = queueRepository.getQueueDescriptor(name);

        long startPipe = qDesc.getPopStartPipe() - 1;
        startPipe = 0 <= startPipe ? startPipe : 0;
        long endPipe = qDesc.getPushStartPipe() + envProps.getNumPipes();

        for (Long pipeNum = startPipe; pipeNum <= endPipe; pipeNum++) {
            addPipe(pipeNum);
        }

        createPopKeyList(qDesc.getPopStartPipe(), startPipe + envProps.getNumPipes() - 1);

        int count = queueRepository.getCount(getName(), startPipe, endPipe);
        this.msgCountCurrent.set(count);

        pipeWatcher = new PipeWatcher();
        new Thread(pipeWatcher).start();
    }

    private void addPipe(Long pipeNum) {
        pipeCountCurrent.put(pipeNum, new AtomicLong());
        pipeMonitorObjMap.put(pipeNum, new Object());
        pipeUseLockMap.put(pipeNum, new AtomicBoolean());

        Bytes rowKey = Bytes.fromUTF8(QueueRepository.formatKey(getName(), pipeNum));

        pipeDescLookupMap.put(rowKey, new PipeDescriptor(pipeNum, rowKey));
        popAllKeyList.add(rowKey);

    }

    private void removePipe(Long pipeNum) {
        pipeCountCurrent.remove(pipeNum);
        pipeMonitorObjMap.remove(pipeNum);
        pipeUseLockMap.remove(pipeNum);

        Bytes rowKey = Bytes.fromUTF8(QueueRepository.formatKey(getName(), pipeNum));

        pipeDescLookupMap.remove(rowKey);
        popAllKeyList.remove(rowKey);

    }

    // private void createPipeDescMap(long startPipe, long endPipe) {
    // Map<Bytes, PipeDescriptor> tmpMap = new HashMap<Bytes, PipeDescriptor>();
    //
    // // create pipe descriptors for ALL active pipes
    // for (long pipeNum = startPipe; pipeNum <= endPipe; pipeNum++) {
    // Bytes rowKey = Bytes.fromUTF8(QueueRepository.formatKey(name, pipeNum));
    // tmpMap.put(rowKey, new PipeDescriptor(pipeNum, rowKey));
    // }
    //
    // pipeDescLookupMap = tmpMap;
    // }

    private void createPopKeyList(long startPipe, long endPipe) {
        List<Bytes> tmpList = new ArrayList<Bytes>((int) (endPipe - startPipe + 1));
        for (long pipeNum = startPipe; pipeNum <= endPipe; pipeNum++) {
            Bytes rowKey = Bytes.fromUTF8(QueueRepository.formatKey(name, pipeNum));
            tmpList.add(rowKey);
        }

        popAllKeyList = tmpList;
    }

    /**
     * Push a value onto the Queue. Choose in round robin fashion the next pipe.
     * 
     * @param value
     *            String value to persist in queue
     * @throws Exception
     */
    public void push(String value) throws Exception {
        long start = System.currentTimeMillis();
        long pipeNum = getNextPushPipeAndInc();
        UUID timeUuid = UUIDGen.makeType1UUIDFromHost(inetAddr);
        queueRepository.insert(QueueRepository.WAITING_COL_FAM, getName(), pipeNum, Bytes.fromUuid(timeUuid),
                Bytes.fromUTF8(value));
        pushCountTotal.incrementAndGet();
        msgCountCurrent.incrementAndGet();
        incPipeCountCurrent(pipeNum);
        pushTimes.addSample(System.currentTimeMillis() - start);
    }

    private void incPipeCountCurrent(Long pipeNum) {
        pipeCountCurrent.get(pipeNum).incrementAndGet();
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

        // popLock.lock();
        // popLockWaitTimes.addSample(System.currentTimeMillis() - start);
        //
        // try {
        // CassQMsg qMsg = nearFifoPop( int pipeNum);
        // return qMsg;
        // }
        // finally {
        // popLock.unlock();
        // }
    }

    private CassQMsg nearFifoPop() throws Exception {
        Bytes rowKey = null;
        Column col = null;

        for (int i = 0; i < envProps.getNumPipes(); i++) {
            Long pipeNum = lockNextFreePopPipe();
            try {
                logger.debug("chose read pipe = " + pipeNum);

                // can optimize the following to not block if the pipe is in use
                // use a test and set approach on atomics representing the set
                // of
                // pipes. if test and set succeeds then use the pipe, if not
                // then
                // move along keep doing this until data found or 'numPipes'
                // fail.
                // doesn't guarantee data will be returned if exists, but the
                // alternative is equally piss poor
                long startPopLockWait = System.currentTimeMillis();
                synchronized (getPipeMonitor(pipeNum)) {
                    popLockWaitTimes.addSample(System.currentTimeMillis() - startPopLockWait);
                    long start = System.currentTimeMillis();
                    List<Column> colList = queueRepository.getWaitingMessages(getName(), pipeNum, 1);
                    getWaitingMsgTimes.addSample(System.currentTimeMillis() - start);
                    if (!colList.isEmpty()) {
                        rowKey = Bytes.fromUTF8(QueueRepository.formatKey(name, pipeNum));
                        col = colList.get(0);
                        moveToDelivered(rowKey, col);
                        break;
                    }
                    else {
                        if (pipeNum < qDesc.getPushStartPipe() - 1) {
                            incrementPopStartPipe();
                        }
                    }
                }
            }
            finally {
                releasePopPipe(pipeNum);
            }
        }

        // if no "oldest", then return null
        if (null == rowKey) {
            return null;
        }

        UUID colName = UUIDGen.makeType1UUID(col.getName());
        return new CassQMsg(new String(rowKey.getBytes()), colName, new String(col.getValue()));
    }

    private Object getPipeMonitor(Long pipeNum) {
        return pipeMonitorObjMap.get(pipeNum);
    }

    private CassQMsg strictFifoPop() throws Exception {
        Bytes rowKey = null;
        Column col = null;
        long start = System.currentTimeMillis();
        Map<Bytes, List<Column>> colList = queueRepository.getOldestFromAllPipes(popAllKeyList);
        getWaitingMsgTimes.addSample(System.currentTimeMillis() - start);

        UUID oldestColName = null;
        // determine which result is the oldest across the queue rows
        for (Entry<Bytes, List<Column>> entry : colList.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }

            Column tmpCol = entry.getValue().get(0);
            UUID colName = UUIDGen.makeType1UUID(tmpCol.getName());
            if (null == rowKey || -1 == colName.compareTo(oldestColName)) {
                rowKey = entry.getKey();
                col = tmpCol;
                oldestColName = colName;
            }
        }

        // if no "oldest", then return null
        if (null == rowKey) {
            return null;
        }

        moveToDelivered(rowKey, col);

        UUID colName = UUIDGen.makeType1UUID(col.getName());
        return new CassQMsg(new String(rowKey.getBytes()), colName, new String(col.getValue()));
    }

    private void moveToDelivered(Bytes rowKey, Column col) throws Exception {
        PipeDescriptor pipeDesc = pipeDescLookupMap.get(rowKey);
        // decPipeCountCurrent(pipeDesc.getPipeNum());

        long moveStart = System.currentTimeMillis();
        queueRepository.moveFromWaitingToDelivered(rowKey, Bytes.fromBytes(col.getName()),
                Bytes.fromBytes(col.getValue()));
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
        queueRepository.removeFromDelivered(Bytes.fromUTF8(qMsg.getQueuePipeKey()), Bytes.fromUuid(qMsg.getMsgId()));
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
        queueRepository.moveFromDeliveredToWaiting(Bytes.fromUTF8(qMsg.getQueuePipeKey()),
                Bytes.fromUuid(qMsg.getMsgId()), Bytes.fromUTF8(qMsg.getValue()));
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

        queueRepository.truncateQueueData(this);
        qDesc = queueRepository.createQueue(getName(), envProps.getNumPipes());

        // release ZK lock region
    }

    private Long lockNextFreePopPipe() throws Exception {
        long start = System.currentTimeMillis();

        try {
            for (;;) {
                Long ret;
                synchronized (popPipeIncMonitor) {
                    ret = nextPopPipeOffset + qDesc.getPopStartPipe();
                    // we do the numPipes+1 to give overlap if the pushers have
                    // already incremented their start pipe.
                    nextPopPipeOffset = (nextPopPipeOffset + 1) % (envProps.getNumPipes() + 1);
                }

                AtomicBoolean tmpBoo = pipeUseLockMap.get(ret);
                if (null != tmpBoo && tmpBoo.compareAndSet(false, true)) {
                    return ret;
                }
            }
        }
        finally {
            getNextPopPipeTimes.addSample(System.currentTimeMillis() - start);
        }
    }

    private void releasePopPipe(Long pipeNum) {
        AtomicBoolean tmpBoo = pipeUseLockMap.get(pipeNum);
        if (null != tmpBoo) {
            tmpBoo.set(false);
        }
    }

    private long getNextPushPipeAndInc() throws Exception {
        long start = System.currentTimeMillis();
        synchronized (pushPipeIncMonitor) {
            long ret = nextPushPipeToUse + qDesc.getPushStartPipe();
            nextPushPipeToUse = (nextPushPipeToUse + 1) % envProps.getNumPipes();
            getNextPushPipeTimes.addSample(System.currentTimeMillis() - start);
            return ret;
        }
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
        return queueRepository.getWaitingMessages(getName(), pipeNum, maxMessags);
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
        return queueRepository.getDeliveredMessages(getName(), pipeNum, maxMessages);
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
        String[] counts = new String[pipeCountCurrent.size()];
        int i = 0;
        for (Entry<Long, AtomicLong> entry : pipeCountCurrent.entrySet()) {
            counts[i++] = entry.getKey() + " = " + entry.getValue().get();
        }
        return counts;
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
        return qDesc.getPopStartPipe();
    }

    public long getPushStartPipe() {
        return qDesc.getPushStartPipe();
    }

    private void incrementPushStartPipe() throws Exception {
        synchronized (pipeManipulateMonitorObj) {
            long tmp = qDesc.getPushStartPipe() + 1;

            addPipe(tmp + envProps.getNumPipes());

            queueRepository.setPushStartPipe(getName(), tmp);

            // don't update qDesc until after the map is updated
            qDesc.setPushStartPipe(tmp);
        }
    }

    private void incrementPopStartPipe() throws Exception {
        long tmp = qDesc.getPopStartPipe() + 1;
        queueRepository.setPopStartPipe(getName(), tmp);
        qDesc.setPopStartPipe(tmp);
        removePipe(tmp - 1);
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
        pipeWatcher.setContinueProcessing(false);
    }

    /**
     * Determines when to increase the start pipe for pushing new messages.
     * 
     * @author Todd Burruss
     */
    class PipeWatcher implements Runnable {
        private long sleepTime = 100;
        private long lastPushPipeInctime = 0;
        private boolean continueProcessing = true;
        private Thread theThread;

        @Override
        public void run() {
            theThread = Thread.currentThread();
            theThread.setName(getClass().getSimpleName());
            lastPushPipeInctime = System.currentTimeMillis();
            while (continueProcessing) {
                try {
                    qDesc = queueRepository.getQueueDescriptor(getName());
                    if (System.currentTimeMillis() - lastPushPipeInctime > envProps.getPushPipeIncrementDelay()) {
                        try {
                            incrementPushStartPipe();
                            lastPushPipeInctime = System.currentTimeMillis();
                        }
                        catch (Throwable e) {
                            logger.error("exception while incrementing push start pipe", e);
                        }
                    }
                    // read again to get latest data
                    qDesc = queueRepository.getQueueDescriptor(getName());

                }
                catch (Exception e) {
                    logger.error("exception while getting queue descriptor", e);
                }

                try {
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        }

        public boolean isContinueProcessing() {
            return continueProcessing;
        }

        public void setContinueProcessing(boolean continueProcessing) {
            this.continueProcessing = continueProcessing;
            theThread.interrupt();
        }
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
