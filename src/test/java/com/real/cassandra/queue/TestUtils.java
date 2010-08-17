package com.real.cassandra.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.wyki.cassandra.pelops.GeneralPolicy;
import org.wyki.cassandra.pelops.ThriftPoolComplex.Policy;

import com.real.cassandra.queue.repository.PelopsPool;
import com.real.cassandra.queue.repository.QueueRepository;

public class TestUtils {
    public static final String POOL_NAME = "myTestPool";
    public static final String QUEUE_NAME = "myTestQueue";
    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    public static final List<String> NODE_LIST = Arrays.asList(new String[] {
        "localhost" });
    public static final int THRIFT_PORT = 9160;
    public static final int REPLICATION_FACTOR = 1;
    // public static final List<String> NODE_LIST = Arrays.asList(new String[] {
    // "172.27.109.32", "172.27.109.33", "172.27.109.34", "172.27.109.35" });
    // public static final int THRIFT_PORT = 9161;
    // public static final int REPLICATION_FACTOR = 3;

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

    public static PelopsPool createPelopsPool(int minConns, int maxConns) {
        Policy policy = new Policy();
        policy.setFramedTransportRequired(true);
        policy.setKillNodeConnsOnException(true);
        policy.setMaxConnectionsPerNode(maxConns);
        policy.setMinCachedConnectionsPerNode(minConns);
        policy.setTargetConnectionsPerNode((minConns + maxConns) / 2);

        GeneralPolicy genPolicy = new GeneralPolicy();
        genPolicy.setMaxOpRetries(10);

        PelopsPool pool = new PelopsPool();
        pool.setGeneralPolicy(genPolicy);
        pool.setHostNameList(NODE_LIST);
        pool.setPort(THRIFT_PORT);
        pool.setKeyspaceName(QueueRepository.KEYSPACE_NAME);
        pool.setNodeDiscovery(false);
        pool.setPolicy(policy);
        pool.setPoolName(TestUtils.POOL_NAME);
        pool.setThriftFramedTransport(true);

        pool.initPool();
        return pool;
    }
}
