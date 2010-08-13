package com.real.cassandra.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.cassandra.thrift.Column;
import org.wyki.cassandra.pelops.Bytes;
import org.wyki.cassandra.pelops.UuidHelper;

import com.real.cassandra.queue.repository.QueueRepository;

/**
 * Implementation of a simple FIFO queue using Cassandra as persistent storage.
 * No caching or priorities are implemented to keep it easy and simple.
 * 
 * <p/>
 * Uses multiple keys per key (I call them "pipes") to help distribute data
 * across the cluster.
 * <p/>
 * {@link #push(String)} will push a value onto one of the pipes in the queue.
 * The pipe is chosen in round robin fashion.
 * <p/>
 * {@link #pop()} will read from all pipes and return the oldest message.
 * 
 * @author Todd Burruss
 */
public class CassQueue {
    // private static final Bytes EMPTY_STRING_BYTES = Bytes.fromUTF8("");

    private QueueRepository queueRepository;
    private String name;
    private int numPipes;
    private int nextPipeToUse = 0;
    private Object pipeIncMonitor = new Object();
    private List<Bytes> queuePipeKeyList;

    /**
     * 
     * @param queueRepository
     *            Repository used to communicate to/from Cassandra cluster
     * @param name
     *            Name of the Queue
     * @param numPipes
     *            The width or number of "rows" the queue uses internally to
     *            help distibute data across cluster.
     */
    public CassQueue(QueueRepository queueRepository, String name, int numPipes) {
        this.queueRepository = queueRepository;

        this.name = name;
        this.numPipes = numPipes;

        queuePipeKeyList = new ArrayList<Bytes>(numPipes);
        for (int i = 0; i < numPipes; i++) {
            queuePipeKeyList.add(Bytes.fromUTF8(QueueRepository.formatKey(name, i)));
        }
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

    /**
     * Push a value onto the Queue. Choose in round robin fashion the next pipe.
     * 
     * @param value
     *            String value to persist in queue
     * @throws Exception
     */
    public void push(String value) throws Exception {
        UUID timeUuid = UuidHelper.newTimeUuid();
        queueRepository.insert(QueueRepository.WAITING_COL_FAM, getName(), getNextPipeAndInc(),
                Bytes.fromUuid(timeUuid), Bytes.fromUTF8(value));
    }

    /**
     * Pop the oldest message from the queue. If no messages, null is returned.
     * 
     * @return Oldest message in the queue, or null if no messages.
     * @throws Exception
     */
    public CassQMsg pop() throws Exception {

        // enter ZK lock region

        Map<Bytes, List<Column>> colList = queueRepository.getOldestFromAllPipes(queuePipeKeyList);

        // determine which result is the oldest across the queue rows
        Bytes rowKey = null;
        UUID oldestColName = null;
        byte[] oldestColValue = null;
        for (Entry<Bytes, List<Column>> entry : colList.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }

            Column tmpCol = entry.getValue().get(0);
            UUID colName = UuidHelper.timeUuidFromBytes(tmpCol.getName());
            if (null == rowKey || -1 == colName.compareTo(oldestColName)) {
                rowKey = entry.getKey();
                oldestColName = colName;
                oldestColValue = tmpCol.getValue();
            }
        }

        // if no "oldest", then return null
        if (null == rowKey) {
            return null;
        }

        queueRepository.moveFromWaitingToDelivered(rowKey, Bytes.fromUuid(oldestColName),
                Bytes.fromBytes(oldestColValue));

        // release ZK lock region

        return new CassQMsg(new String(rowKey.getBytes()), oldestColName, new String(oldestColValue));
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

        synchronized (pipeIncMonitor) {
            queueRepository.truncateQueue(this);
            nextPipeToUse = 0;
        }

        // release ZK lock region
    }

    private int getNextPipeAndInc() {
        synchronized (pipeIncMonitor) {
            int ret = nextPipeToUse;
            nextPipeToUse = (nextPipeToUse + 1) % numPipes;
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

}
