package com.real.cassandra.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.CachePerNodePool.Policy;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.OperandPolicy;

import com.real.cassandra.queue.repository.PelopsPool;
import com.real.cassandra.queue.repository.QueueRepository;

public class TestUtils {
    public static final String QUEUE_POOL_NAME = "myTestPool";
    public static final String SYSTEM_POOL_NAME = "mySystemPool";
    public static final String QUEUE_NAME = "myTestQueue";
    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    public static final String[] NODE_LIST = new String[] {
        "localhost" };
    public static final int REPLICATION_FACTOR = 1;

    // public static final String[] NODE_LIST = new String[] {
    // "172.27.109.32", "172.27.109.33", "172.27.109.34", "172.27.109.35" };
    // public static final int REPLICATION_FACTOR = 3;

    // public static final String[] NODE_LIST = new String[] {
    // "172.27.109.32" );
    // public static final int REPLICATION_FACTOR = 1;

    public static final int THRIFT_PORT = 9161;

    private CassQueue cq;

    public TestUtils(CassQueue cq) {
        this.cq = cq;
    }

    public String outputEventsAsCommaDelim(Collection<CassQMsg> collection) {
        if (null == collection) {
            return null;
        }

        if (collection.isEmpty()) {
            return "";
        }

        StringBuilder sb = null;
        for (CassQMsg evt : collection) {
            if (null != sb) {
                sb.append(", ");
            }
            else {
                sb = new StringBuilder();
            }

            sb.append(evt.getValue());
        }

        return sb.toString();
    }

    public String outputStringsAsCommaDelim(Collection<String> collection) {
        if (null == collection) {
            return null;
        }

        if (collection.isEmpty()) {
            return "";
        }

        StringBuilder sb = null;
        for (String str : collection) {
            if (null != sb) {
                sb.append(", ");
            }
            else {
                sb = new StringBuilder();
            }

            sb.append(str);
        }

        return sb.toString();
    }

    public String outputColumnsAsCommaDelim(Collection<Column> collection) {
        if (null == collection) {
            return null;
        }

        if (collection.isEmpty()) {
            return "";
        }

        StringBuilder sb = null;
        for (Column col : collection) {
            if (null != sb) {
                sb.append(", ");
            }
            else {
                sb = new StringBuilder();
            }

            sb.append(new String(col.getValue()));
        }

        return sb.toString();
    }

    public void verifyExistsInDeliveredQueue(int index, int numEvents, boolean wantExists) throws Exception {
        List<Column> colList = cq.getDeliveredMessages(index % cq.getNumPipes(), numEvents + 1);
        if (wantExists) {
            boolean found = false;
            for (Column col : colList) {
                if (new String(col.getValue()).equals("xxx_" + index)) {
                    found = true;
                    break;
                }
            }
            assertTrue("should have found value, xxx_" + index + " in delivered queue", found);
        }
        else {
            for (Column col : colList) {
                assertNotSame(new String(col.getValue()), "xxx_" + index);
            }
        }

    }

    public void verifyExistsInWaitingQueue(int index, int numEvents, boolean wantExists) throws Exception {
        List<Column> colList = cq.getWaitingMessages(index % cq.getNumPipes(), numEvents + 1);
        if (wantExists) {
            boolean found = false;
            for (Column col : colList) {
                if (new String(col.getValue()).equals("xxx_" + index)) {
                    found = true;
                    break;
                }
            }
            assertTrue("should have found value, xxx_" + index + " in waiting queue", found);
        }
        else {
            for (Column col : colList) {
                assertNotSame(new String(col.getValue()), "xxx_" + index);
            }
        }

    }

    public void verifyDeliveredQueue(int numEvents) throws Exception {
        int min = numEvents / cq.getNumPipes();
        int mod = numEvents % cq.getNumPipes();

        for (int i = 0; i < cq.getNumPipes(); i++) {
            List<Column> colList = cq.getDeliveredMessages(i, numEvents + 1);
            assertEquals("count on queue index " + i + " is incorrect", i < mod ? min + 1 : min, colList.size());

            for (int j = 0; j < colList.size(); j++) {
                String value = new String(colList.get(j).getValue());
                assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
            }
        }
    }

    public void verifyWaitingQueue(int numEvents) throws Exception {
        int min = numEvents / cq.getNumPipes();
        int mod = numEvents % cq.getNumPipes();

        for (int i = 0; i < cq.getNumPipes(); i++) {
            List<Column> colList = cq.getWaitingMessages(i, numEvents + 1);
            assertEquals("count on queue index " + i + " is incorrect: events = " + outputColumnsAsCommaDelim(colList),
                    i < mod ? min + 1 : min, colList.size());

            for (int j = 0; j < colList.size(); j++) {
                String value = new String(colList.get(j).getValue());
                assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
            }
        }
    }

    public String formatMsgValue(String base, int pipeNum) {
        return base + "-" + pipeNum;
    }

    public static PelopsPool createQueuePool(int minConns, int maxConns) {
        Cluster cluster = new Cluster(NODE_LIST, THRIFT_PORT);
        cluster.setFramedTransportRequired(true);

        Policy policy = new Policy();
        policy.setKillNodeConnsOnException(true);
        policy.setMaxConnectionsPerNode(maxConns);
        policy.setMinCachedConnectionsPerNode(minConns);
        policy.setTargetConnectionsPerNode((minConns + maxConns) / 2);

        OperandPolicy opPolicy = new OperandPolicy();
        opPolicy.setMaxOpRetries(10);

        PelopsPool pool = new PelopsPool();
        pool.setCluster(cluster);
        pool.setOperandPolicy(opPolicy);
        pool.setKeyspaceName(QueueRepository.QUEUE_KEYSPACE_NAME);
        pool.setNodeDiscovery(false);
        pool.setPolicy(policy);
        pool.setPoolName(TestUtils.QUEUE_POOL_NAME);

        pool.initPool();
        return pool;
    }

    public static PelopsPool createSystemPool() {
        Cluster cluster = new Cluster(NODE_LIST, THRIFT_PORT);
        cluster.setFramedTransportRequired(true);

        Policy policy = new Policy();
        policy.setKillNodeConnsOnException(true);
        policy.setMaxConnectionsPerNode(1);
        policy.setMinCachedConnectionsPerNode(0);
        policy.setTargetConnectionsPerNode(0);

        OperandPolicy opPolicy = new OperandPolicy();
        opPolicy.setMaxOpRetries(10);

        PelopsPool pool = new PelopsPool();
        pool.setCluster(cluster);
        pool.setOperandPolicy(opPolicy);
        pool.setKeyspaceName(QueueRepository.SYSTEM_KEYSPACE_NAME);
        pool.setNodeDiscovery(false);
        pool.setPolicy(policy);
        pool.setPoolName(TestUtils.SYSTEM_POOL_NAME);

        pool.initPool();
        return pool;
    }

    public boolean isPushPopOpFinished(Set<PushPopAbstractBase> opSet) {
        for (PushPopAbstractBase cqOp : opSet) {
            if (!cqOp.isFinished()) {
                return false;
            }
        }
        return true;
    }

    public void reportPopStatus(Set<PushPopAbstractBase> popperSet, Queue<CassQMsg> popQueue) {
        long elapsed = 0;
        int totalPopped = 0;
        for (PushPopAbstractBase popper : popperSet) {
            totalPopped += popper.getMsgsProcessed();
            long tmp = popper.getElapsedTime();
            if (tmp > elapsed) {
                elapsed = tmp;
            }
        }

        double secs = elapsed / 1000.0;
        System.out.println("current elapsed pop time : " + secs + " (" + totalPopped + " : " + totalPopped / secs
                + "p/s)");
    }

    public Set<PushPopAbstractBase> startPushers(CassQueue cq, String baseValue, int numPushers, int numToPush,
            long pushDelay) {
        Set<PushPopAbstractBase> retSet = new HashSet<PushPopAbstractBase>();
        for (int i = 0; i < numPushers; i++) {
            CassQueuePusher cqPusher = new CassQueuePusher(cq, baseValue + "-" + i);
            retSet.add(cqPusher);
            cqPusher.start(numToPush, pushDelay);
        }

        return retSet;
    }

    public Set<PushPopAbstractBase> startPoppers(CassQueue cq, String baseValue, int numPoppers, int numToPop,
            long popDelay, Queue<CassQMsg> popQ) {
        Set<PushPopAbstractBase> retSet = new HashSet<PushPopAbstractBase>();
        for (int i = 0; i < numPoppers; i++) {
            CassQueuePopper cqPopper = new CassQueuePopper(cq, baseValue, popQ);
            retSet.add(cqPopper);
            cqPopper.start(numToPop, popDelay);
        }

        return retSet;
    }

}
