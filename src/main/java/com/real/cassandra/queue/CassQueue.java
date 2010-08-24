package com.real.cassandra.queue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

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
public class CassQueue {
    private static Logger logger = LoggerFactory.getLogger(CassQueue.class);

    private final InetAddress inetAddr;

    private QueueRepository queueRepository;
    private String name;
    private int numPipes;
    private int nextReadPipeToUse = 0;
    private int nextWritePipeToUse = 0;
    private Object writePipeIncMonitor = new Object();
    private Object readPipeIncMonitor = new Object();
    private List<Bytes> queuePipeKeyList;
    private boolean nearFifoOk = true;
    private PopLock popLock;

    /**
     * 
     * @param queueRepository
     *            Repository used to communicate to/from Cassandra cluster
     * @param name
     *            Name of the Queue
     * @param numPipes
     *            The width or number of "rows" the queue uses internally to
     *            help distibute data across cluster.
     * @param popLocks
     *            Client desires <code>pop</code>s to be locked so as to allow
     *            only one thread in pop routine at a time. Locking mechanism
     *            depends on the value of <code>distributed</code>.
     * @param distributed
     *            Whether or not utilize cross JVM locking capability (Uses
     *            ZooKeeper.)
     */
    public CassQueue(QueueRepository queueRepository, String name, int numPipes, boolean popLocks, boolean distributed) {
        try {
            inetAddr = InetAddress.getLocalHost();
        }
        catch (UnknownHostException e) {
            logger.error("exception while getting local IP address", e);
            throw new RuntimeException(e);
        }

        this.queueRepository = queueRepository;

        this.name = name;
        this.numPipes = numPipes;

        if (null == name) {
            throw new IllegalArgumentException("queue name cannot be null");
        }

        if (popLocks) {
            if (distributed) {
                popLock = new PopLockDistributedImpl(this.name);
            }
            else {
                popLock = new PopLockLocalImpl();
            }
        }
        else {
            popLock = new PopLockNoOpImpl();
        }

        queuePipeKeyList = new ArrayList<Bytes>(numPipes);
        for (int i = 0; i < numPipes; i++) {
            queuePipeKeyList.add(Bytes.fromUTF8(QueueRepository.formatKey(name, i)));
        }
    }

    /**
     * Push a value onto the Queue. Choose in round robin fashion the next pipe.
     * 
     * @param value
     *            String value to persist in queue
     * @throws Exception
     */
    public void push(String value) throws Exception {
        UUID timeUuid = UUIDGen.makeType1UUIDFromHost(inetAddr);
        queueRepository.insert(QueueRepository.WAITING_COL_FAM, getName(), getNextWritePipeAndInc(),
                Bytes.fromUuid(timeUuid), Bytes.fromUTF8(value));
    }

    /**
     * Pop the oldest message from the queue. If no messages, null is returned.
     * 
     * @return Oldest message in the queue, or null if no messages.
     * @throws Exception
     */
    public CassQMsg pop() throws Exception {
        popLock.lock();

        try {
            return lockedPop();
        }
        finally {
            popLock.unlock();
        }
    }

    private CassQMsg lockedPop() throws Exception {
        Bytes rowKey = null;
        Column col = null;

        if (nearFifoOk) {
            for (int i = 4; 0 < i; i--) {
                int tmp = getNextReadPipeAndInc();
                logger.debug("chose read pipe = " + tmp);
                List<Column> colList = queueRepository.getWaitingMessages(getName(), tmp, 1);
                if (!colList.isEmpty()) {
                    rowKey = Bytes.fromUTF8(QueueRepository.formatKey(name, tmp));
                    col = colList.get(0);
                    break;
                }
            }
        }
        else {
            Map<Bytes, List<Column>> colList = queueRepository.getOldestFromAllPipes(queuePipeKeyList);

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
        }

        // if no "oldest", then return null
        if (null == rowKey) {
            return null;
        }

        queueRepository.moveFromWaitingToDelivered(rowKey, Bytes.fromBytes(col.getName()),
                Bytes.fromBytes(col.getValue()));

        UUID colName = UUIDGen.makeType1UUID(col.getName());
        return new CassQMsg(new String(rowKey.getBytes()), colName, new String(col.getValue()));
    }

    /**
     * Commit the 'pop' of a previous message. If commit is not called, the
     * message will be rolled back ({@link #rollback(CassQMsg)}).
     * 
     * @param qMsg
     * @throws Exception
     */
    public void commit(CassQMsg qMsg) throws Exception {
        queueRepository.removeFromDelivered(Bytes.fromUTF8(qMsg.getQueuePipeKey()), Bytes.fromUuid(qMsg.getMsgId()));
    }

    /**
     * Rollback a message. This means the message will be available again and
     * eventually pop'ed from the queue.
     * 
     * @param qMsg
     * @throws Exception
     */
    public void rollback(CassQMsg qMsg) throws Exception {
        queueRepository.moveFromDeliveredToWaiting(Bytes.fromUTF8(qMsg.getQueuePipeKey()),
                Bytes.fromUuid(qMsg.getMsgId()), Bytes.fromUTF8(qMsg.getValue()));
    }

    /**
     * Remove all data from the queue. This includes all waiting and uncommitted
     * data. In addition, the next pipe to use is reset.
     * 
     * @throws Exception
     */
    public void truncate() throws Exception {
        // enter ZK lock region

        queueRepository.truncateQueue(this);
        nextReadPipeToUse = 0;
        nextWritePipeToUse = 0;

        // release ZK lock region
    }

    private int getNextReadPipeAndInc() {
        synchronized (readPipeIncMonitor) {
            int ret = nextReadPipeToUse;
            nextReadPipeToUse = (nextReadPipeToUse + 1) % numPipes;
            return ret;
        }
    }

    private int getNextWritePipeAndInc() {
        synchronized (writePipeIncMonitor) {
            int ret = nextWritePipeToUse;
            nextWritePipeToUse = (nextWritePipeToUse + 1) % numPipes;
            logger.debug("chose write pipe = " + ret);
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
    public List<Column> getWaitingMessages(int pipeNum, int maxMessags) throws Exception {
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
    public List<Column> getDeliveredMessages(int pipeNum, int maxMessages) throws Exception {
        return queueRepository.getDeliveredMessages(getName(), pipeNum, maxMessages);
    }

    /**
     * Return status of near FIFO.
     * 
     * @return
     */
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
    public String getName() {
        return name;
    }

    /**
     * Return number of pipes.
     * 
     * @return
     */
    public int getNumPipes() {
        return numPipes;
    }

}
