package com.real.cassandra.queue.roundrobin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.CachePerNodePool.Policy;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.OperandPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.repository.PelopsPool;
import com.real.cassandra.queue.roundrobin.CassQueueFactoryImpl;
import com.real.cassandra.queue.roundrobin.CassQueueImpl;
import com.real.cassandra.queue.roundrobin.PipeManagerImpl;
import com.real.cassandra.queue.roundrobin.QueueRepositoryImpl;

public class TestUtils {
    private static Logger logger = LoggerFactory.getLogger(TestUtils.class);

    public static final String QUEUE_POOL_NAME = "myTestPool";
    public static final String SYSTEM_POOL_NAME = "mySystemPool";
    public static final String QUEUE_NAME = "myTestQueue";
    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    private static PelopsPool queuePool;

    private CassQueueImpl cq;

    public TestUtils(CassQueueImpl cq) {
        this.cq = cq;
    }

    public boolean monitorPushersPoppers(Queue<CassQMsg> popQ, List<PushPopAbstractBase> pusherSet,
            List<PushPopAbstractBase> popperSet, Set<CassQMsg> msgSet, Set<String> valueSet) {
        //
        // process popped messages and wait until finished - make sure a message
        // is only processed once
        //

        logger.info("start monitoring for end-of-test conditions");

        // long start = System.currentTimeMillis();
        long interval = System.currentTimeMillis();
        while (!popQ.isEmpty() || !isPushPopOpFinished(popperSet) || !isPushPopOpFinished(pusherSet)) {
            CassQMsg qMsg = !popQ.isEmpty() ? popQ.remove() : null;
            if (null != qMsg) {
                if (null != msgSet && !msgSet.add(qMsg)) {
                    fail("msg already popped - either message pushed twice or popped twice : " + qMsg.toString());
                }
                if (null != valueSet && !valueSet.add(qMsg.getValue())) {
                    fail("value of message pushed more than once : " + qMsg.toString());
                }
            }
            else {
                try {
                    Thread.sleep(200);
                }
                catch (InterruptedException e) {
                    // do nothing
                }
            }

            if (1000 < (System.currentTimeMillis() - interval)) {
                reportPopStatus(popperSet, popQ);
                interval = System.currentTimeMillis();
            }
        }

        logger.info("final pop stats");
        reportPopStatus(popperSet, popQ);

        return true;
    }

    private static void fail(String msg) {
        throw new RuntimeException(msg);
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

    public void verifyExistsInWaitingQueue(int pipeNum, int numEvents, boolean wantExists, PipeManagerImpl pipeMgr)
            throws Exception {
        List<Column> colList =
                cq.getWaitingMessages(pipeMgr.getPipeDescriptor(pipeNum % cq.getNumPipes()), numEvents + 1);
        if (wantExists) {
            boolean found = false;
            for (Column col : colList) {
                if (new String(col.getValue()).equals("xxx_" + pipeNum)) {
                    found = true;
                    break;
                }
            }
            assertTrue("should have found value, xxx_" + pipeNum + " in waiting queue", found);
        }
        else {
            for (Column col : colList) {
                assertNotSame(new String(col.getValue()), "xxx_" + pipeNum);
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

    public void verifyWaitingQueue(int numEvents, PipeManagerImpl pipeMgr) throws Exception {
        int min = numEvents / cq.getNumPipes();
        int mod = numEvents % cq.getNumPipes();

        for (int i = 0; i < cq.getNumPipes(); i++) {
            List<Column> colList = cq.getWaitingMessages(pipeMgr.getPipeDescriptor(i), numEvents + 1);
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

    public static PelopsPool createQueuePool(String[] hostArr, int thriftPort, boolean useFramedTransport,
            int minCachedConns, int maxConns, int targetConns, boolean killNodeConnsOnException) {
        Cluster cluster = new Cluster(hostArr, thriftPort);
        cluster.setFramedTransportRequired(useFramedTransport);

        Policy policy = new Policy();
        policy.setKillNodeConnsOnException(true);
        policy.setMaxConnectionsPerNode(maxConns);
        policy.setMinCachedConnectionsPerNode(minCachedConns);
        policy.setTargetConnectionsPerNode(targetConns);
        policy.setKillNodeConnsOnException(killNodeConnsOnException);

        OperandPolicy opPolicy = new OperandPolicy();
        opPolicy.setMaxOpRetries(10);

        PelopsPool pool = new PelopsPool();
        pool.setCluster(cluster);
        pool.setOperandPolicy(opPolicy);
        pool.setKeyspaceName(QueueRepositoryImpl.QUEUE_KEYSPACE_NAME);
        pool.setNodeDiscovery(false);
        pool.setPolicy(policy);
        pool.setPoolName(TestUtils.QUEUE_POOL_NAME);

        pool.initPool();
        return pool;
    }

    public static PelopsPool createSystemPool(String[] hostArr, int thriftPort, boolean useFramedTransport) {
        Cluster cluster = new Cluster(hostArr, thriftPort);
        cluster.setFramedTransportRequired(useFramedTransport);

        Policy policy = new Policy();
        policy.setKillNodeConnsOnException(true);
        policy.setMaxConnectionsPerNode(10);
        policy.setMinCachedConnectionsPerNode(1);
        policy.setTargetConnectionsPerNode(2);

        OperandPolicy opPolicy = new OperandPolicy();
        opPolicy.setMaxOpRetries(10);

        PelopsPool pool = new PelopsPool();
        pool.setCluster(cluster);
        pool.setOperandPolicy(opPolicy);
        pool.setKeyspaceName(QueueRepositoryImpl.SYSTEM_KEYSPACE_NAME);
        pool.setNodeDiscovery(false);
        pool.setPolicy(policy);
        pool.setPoolName(TestUtils.SYSTEM_POOL_NAME);

        pool.initPool();
        return pool;
    }

    public boolean isPushPopOpFinished(List<PushPopAbstractBase> opList) {
        if (opList.isEmpty()) {
            // assume watcher thread hasn't started any yet
            return false;
        }

        for (PushPopAbstractBase cqOp : opList) {
            if (!cqOp.isFinished()) {
                return false;
            }
        }
        return true;
    }

    public void reportPopStatus(List<PushPopAbstractBase> popperSet, Queue<CassQMsg> popQueue) {
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
        logger.info("current elapsed pop time : " + secs + " (" + totalPopped + " : " + totalPopped / secs + " pop/s)");
    }

    public static QueueRepositoryImpl setupQueueSystemAndPelopsPool(EnvProperties envProps,
            ConsistencyLevel consistencyLevel) throws Exception {
        // must create system pool first and initialize cassandra
        PelopsPool systemPool =
                TestUtils.createSystemPool(envProps.getHostArr(), envProps.getThriftPort(),
                        envProps.getUseFramedTransport());
        QueueRepositoryImpl qRep =
                new QueueRepositoryImpl(systemPool, envProps.getReplicationFactor(), consistencyLevel);
        qRep.initCassandra(envProps.getDropKeyspace());

        queuePool =
                TestUtils.createQueuePool(envProps.getHostArr(), envProps.getThriftPort(),
                        envProps.getUseFramedTransport(), envProps.getMinCacheConnsPerHost(),
                        envProps.getMaxConnectionsPerHost(), envProps.getTargetConnectionsPerHost(),
                        envProps.getKillNodeConnectionsOnException());
        qRep.setQueuePool(queuePool);
        return qRep;
    }

    public static CassQueueImpl setupQueue(QueueRepositoryImpl qRep, String name, EnvProperties envProps,
            boolean popLocks, boolean distributed, PipeManagerImpl pipeMgr) throws Exception {
        CassQueueFactoryImpl cqf = new CassQueueFactoryImpl(qRep);
        CassQueueImpl cq = cqf.createQueueInstance(name, envProps, popLocks, distributed, pipeMgr);
        return cq;
    }

    public List<PushPopAbstractBase> startPushers(CassQueueImpl cq, String baseValue, EnvProperties envProps) {
        List<PushPopAbstractBase> retList = new ArrayList<PushPopAbstractBase>(envProps.getNumPushers());
        WorkerThreadWatcher ptw = new PusherThreadWatcher(envProps, retList, baseValue);
        ptw.start();
        return retList;
    }

    public List<PushPopAbstractBase> startPoppers(CassQueueImpl cq, String baseValue, Queue<CassQMsg> popQ,
            EnvProperties envProps) {
        List<PushPopAbstractBase> retList = new ArrayList<PushPopAbstractBase>(envProps.getNumPoppers());
        WorkerThreadWatcher ptw = new PopperThreadWatcher(envProps, retList, baseValue, popQ);
        ptw.start();
        return retList;
    }

    abstract class WorkerThreadWatcher implements Runnable {
        protected EnvProperties envProps;
        protected List<PushPopAbstractBase> workerList;
        protected String baseValue;

        private Thread theThread;

        public WorkerThreadWatcher(EnvProperties envProps, List<PushPopAbstractBase> workerList, String baseValue) {
            this.envProps = envProps;
            this.workerList = workerList;
            this.baseValue = baseValue;
        }

        public void start() {
            theThread = new Thread(this);
            theThread.setDaemon(true);
            theThread.setName(getClass().getSimpleName());
            theThread.start();
        }

        @Override
        public void run() {
            for (;;) {
                if (getTargetSize() != workerList.size()) {
                    adjustWorkers();
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    logger.error("exception while sleeping - ignoring", e);
                    Thread.interrupted();
                }
            }
        }

        private void adjustWorkers() {
            while (getTargetSize() != workerList.size()) {
                int newSize = getTargetSize();
                int currSize = workerList.size();
                if (newSize < currSize) {
                    PushPopAbstractBase popper = workerList.remove(newSize);
                    popper.setStopProcessing(true);
                }
                else if (newSize > currSize) {
                    addWorker();
                }
            }
        }

        protected abstract int getTargetSize();

        protected abstract void addWorker();
    }

    class PusherThreadWatcher extends WorkerThreadWatcher {
        public PusherThreadWatcher(EnvProperties envProps, List<PushPopAbstractBase> workerList, String baseValue) {
            super(envProps, workerList, baseValue);
        }

        @Override
        protected void addWorker() {
            CassQueuePusher cqPusher = new CassQueuePusher(cq, baseValue + "-" + workerList.size(), envProps);
            workerList.add(cqPusher);
            cqPusher.start(envProps.getNumMsgsPerPusher());

        }

        @Override
        protected int getTargetSize() {
            return envProps.getNumPushers();
        }
    }

    class PopperThreadWatcher extends WorkerThreadWatcher {
        private Queue<CassQMsg> popQ;

        public PopperThreadWatcher(EnvProperties envProps, List<PushPopAbstractBase> workerList, String baseValue,
                Queue<CassQMsg> popQ) {
            super(envProps, workerList, baseValue);
            this.popQ = popQ;
        }

        @Override
        protected void addWorker() {
            CassQueuePopper cqPopper = new CassQueuePopper(cq, baseValue, envProps, popQ);
            workerList.add(cqPopper);
            cqPopper.start(envProps.getNumMsgsPerPopper());

        }

        @Override
        protected int getTargetSize() {
            return envProps.getNumPoppers();
        }
    }
}
