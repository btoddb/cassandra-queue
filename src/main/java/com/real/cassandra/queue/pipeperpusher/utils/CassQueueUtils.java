package com.real.cassandra.queue.pipeperpusher.utils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.transport.TTransportException;
import org.scale7.cassandra.pelops.CachePerNodePool.Policy;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.OperandPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.pipeperpusher.CassQueueImpl;
import com.real.cassandra.queue.pipeperpusher.CassQueuePopper;
import com.real.cassandra.queue.pipeperpusher.CassQueuePusher;
import com.real.cassandra.queue.pipeperpusher.PushPopAbstractBase;
import com.real.cassandra.queue.pipeperpusher.QueueRepositoryImpl;
import com.real.cassandra.queue.repository.PelopsPool;

public class CassQueueUtils {
    private static Logger logger = LoggerFactory.getLogger(CassQueueUtils.class);

    public static final String QUEUE_POOL_NAME = "myTestPool";
    public static final String SYSTEM_POOL_NAME = "mySystemPool";
    public static final String QUEUE_NAME = "myTestQueue";
    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    private static PelopsPool systemPool;
    private static PelopsPool queuePool;

    public CassQueueUtils() {
    }

    public static boolean monitorPushersPoppers(Queue<CassQMsg> popQ, List<PushPopAbstractBase> pusherSet,
            List<PushPopAbstractBase> popperSet, Set<CassQMsg> msgSet, Set<String> valueSet) {
        //
        // process popped messages and wait until finished - make sure a message
        // is only processed once
        //

        logger.info("start monitoring for end-of-test conditions");

        // long start = System.currentTimeMillis();
        long interval = System.currentTimeMillis();
        int lastPopCount = 0;
        while (!popQ.isEmpty() || !isPushPopOpFinished(pusherSet)
                || (!isPushPopOpFinished(popperSet) && lastPopCount != countPops(popperSet))) {
            CassQMsg qMsg = !popQ.isEmpty() ? popQ.remove() : null;
            if (null != qMsg) {
                if (null != msgSet && (msgSet.contains(qMsg) || !msgSet.add(qMsg))) {
                    fail("msg already popped - either message pushed twice or popped twice : " + qMsg.toString());
                }
                if (null != valueSet && (valueSet.contains(qMsg) || !valueSet.add(qMsg.getMsgData()))) {
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

        return popQ.isEmpty() && isPushPopOpFinished(pusherSet) && isPushPopOpFinished(popperSet);
    }

    private static void fail(String msg) {
        throw new RuntimeException(msg);
    }

    // public String outputEventsAsCommaDelim(Collection<CassQMsg> collection) {
    // if (null == collection) {
    // return null;
    // }
    //
    // if (collection.isEmpty()) {
    // return "";
    // }
    //
    // StringBuilder sb = null;
    // for (CassQMsg evt : collection) {
    // if (null != sb) {
    // sb.append(", ");
    // }
    // else {
    // sb = new StringBuilder();
    // }
    //
    // sb.append(evt.getValue());
    // }
    //
    // return sb.toString();
    // }
    //
    // public String outputStringsAsCommaDelim(Collection<String> collection) {
    // if (null == collection) {
    // return null;
    // }
    //
    // if (collection.isEmpty()) {
    // return "";
    // }
    //
    // StringBuilder sb = null;
    // for (String str : collection) {
    // if (null != sb) {
    // sb.append(", ");
    // }
    // else {
    // sb = new StringBuilder();
    // }
    //
    // sb.append(str);
    // }
    //
    // return sb.toString();
    // }
    //
    // public String outputColumnsAsCommaDelim(Collection<Column> collection) {
    // if (null == collection) {
    // return null;
    // }
    //
    // if (collection.isEmpty()) {
    // return "";
    // }
    //
    // StringBuilder sb = null;
    // for (Column col : collection) {
    // if (null != sb) {
    // sb.append(", ");
    // }
    // else {
    // sb = new StringBuilder();
    // }
    //
    // sb.append(new String(col.getValue()));
    // }
    //
    // return sb.toString();
    // }
    //
    // public void verifyExistsInDeliveredQueue(int index, int numEvents,
    // boolean wantExists) throws Exception {
    // List<Column> colList = cq.getDeliveredMessages(index % cq.getNumPipes(),
    // numEvents + 1);
    // if (wantExists) {
    // boolean found = false;
    // for (Column col : colList) {
    // if (new String(col.getValue()).equals("xxx_" + index)) {
    // found = true;
    // break;
    // }
    // }
    // assertTrue("should have found value, xxx_" + index +
    // " in delivered queue", found);
    // }
    // else {
    // for (Column col : colList) {
    // assertNotSame(new String(col.getValue()), "xxx_" + index);
    // }
    // }
    //
    // }
    //

    // public void verifyExistsInWaitingQueue(PipeDescriptorImpl pipeDesc, int
    // numEvents, boolean wantExists)
    // throws Exception {
    // List<Column> colList =
    // cq.getWaitingMessages(pipeMgr.getPipeDescriptor(pipeNum %
    // cq.getNumPipes()), numEvents + 1);
    // if (wantExists) {
    // boolean found = false;
    // for (Column col : colList) {
    // if (new String(col.getValue()).equals("xxx_" + pipeNum)) {
    // found = true;
    // break;
    // }
    // }
    // assertTrue("should have found value, xxx_" + pipeNum +
    // " in waiting queue", found);
    // }
    // else {
    // for (Column col : colList) {
    // assertNotSame(new String(col.getValue()), "xxx_" + pipeNum);
    // }
    // }
    // }

    //
    // }
    //
    // public void verifyDeliveredQueue(int numEvents) throws Exception {
    // int min = numEvents / cq.getNumPipes();
    // int mod = numEvents % cq.getNumPipes();
    //
    // for (int i = 0; i < cq.getNumPipes(); i++) {
    // List<Column> colList = cq.getDeliveredMessages(i, numEvents + 1);
    // assertEquals("count on queue index " + i + " is incorrect", i < mod ? min
    // + 1 : min, colList.size());
    //
    // for (int j = 0; j < colList.size(); j++) {
    // String value = new String(colList.get(j).getValue());
    // assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
    // }
    // }
    // }
    //
    // public void verifyWaitingQueue(int numEvents, PipeManagerImpl pipeMgr)
    // throws Exception {
    // int min = numEvents / cq.getNumPipes();
    // int mod = numEvents % cq.getNumPipes();
    //
    // for (int i = 0; i < cq.getNumPipes(); i++) {
    // List<Column> colList =
    // cq.getWaitingMessages(pipeMgr.getPipeDescriptor(i), numEvents + 1);
    // assertEquals("count on queue index " + i + " is incorrect: events = " +
    // outputColumnsAsCommaDelim(colList),
    // i < mod ? min + 1 : min, colList.size());
    //
    // for (int j = 0; j < colList.size(); j++) {
    // String value = new String(colList.get(j).getValue());
    // assertEquals("xxx_" + (i + (j * cq.getNumPipes())), value);
    // }
    // }
    // }

    public static String formatMsgValue(String base, int pipeNum) {
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
        pool.setPoolName(CassQueueUtils.QUEUE_POOL_NAME);

        pool.initPool();
        return pool;
    }

    private static PelopsPool createSystemPool(String[] hostArr, int thriftPort, boolean useFramedTransport) {
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
        pool.setPoolName(CassQueueUtils.SYSTEM_POOL_NAME);

        pool.initPool();
        return pool;
    }

    public static boolean isPushPopOpFinished(List<PushPopAbstractBase> opList) {
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

    public static void reportPopStatus(List<PushPopAbstractBase> popperSet, Queue<CassQMsg> popQueue) {
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

    private static int countPops(List<PushPopAbstractBase> popperSet) {
        int totalPopped = 0;
        for (PushPopAbstractBase popper : popperSet) {
            totalPopped += popper.getMsgsProcessed();
        }
        return totalPopped;
    }

    public static void startCassandraInstance() throws TTransportException, IOException, InterruptedException,
            SecurityException, IllegalArgumentException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException {
        CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
        cleaner.prepare();
        EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
        try {
            cassandra.init();
        }
        catch (TTransportException e) {
            logger.error("exception while initializing cassandra server", e);
            throw e;
        }
        Thread t = new Thread(cassandra);
        t.setName(cassandra.getClass().getSimpleName());
        t.setDaemon(true);
        t.start();
        logger.info("setup: started embedded cassandra thread");
        Thread.sleep(1000);
    }

    public static QueueRepositoryImpl createQueueRepository(EnvProperties envProps, ConsistencyLevel consistencyLevel)
            throws Exception {
        // must create system pool first and initialize cassandra
        systemPool =
                CassQueueUtils.createSystemPool(envProps.getHostArr(), envProps.getThriftPort(),
                        envProps.getUseFramedTransport());

        // pelops will produce errors after creating this pool if keyspace
        // doesn't exist
        queuePool =
                CassQueueUtils.createQueuePool(envProps.getHostArr(), envProps.getThriftPort(),
                        envProps.getUseFramedTransport(), envProps.getMinCacheConnsPerHost(),
                        envProps.getMaxConnectionsPerHost(), envProps.getTargetConnectionsPerHost(),
                        envProps.getKillNodeConnectionsOnException());

        QueueRepositoryImpl qRepos =
                new QueueRepositoryImpl(systemPool, queuePool, envProps.getReplicationFactor(), consistencyLevel);
        qRepos.initCassandra(envProps.getDropKeyspace());
        return qRepos;
    }

    public static EnvProperties createEnvPropertiesWithDefaults() {
        Properties rawProps = new Properties();
        rawProps.setProperty(EnvProperties.ENV_numPipes, "4");
        // rawProps.setProperty(EnvProperties.ENV_pushPipeIncrementDelay,
        // "20000");
        rawProps.setProperty(EnvProperties.ENV_hosts, "localhost");
        rawProps.setProperty(EnvProperties.ENV_thriftPort, "9161");
        rawProps.setProperty(EnvProperties.ENV_useFramedTransport, "false");
        rawProps.setProperty(EnvProperties.ENV_minCacheConnsPerHost, "1");
        rawProps.setProperty(EnvProperties.ENV_maxConnsPerHost, "20");
        rawProps.setProperty(EnvProperties.ENV_targetConnsPerHost, "10");
        rawProps.setProperty(EnvProperties.ENV_killNodeConnsOnException, "false");
        rawProps.setProperty(EnvProperties.ENV_dropKeyspace, "true");
        rawProps.setProperty(EnvProperties.ENV_replicationFactor, "1");
        rawProps.setProperty(EnvProperties.ENV_dropKeyspace, "true");
        rawProps.setProperty(EnvProperties.ENV_truncateQueue, "true");
        return new EnvProperties(rawProps);
    }

    // public static CassQueueImpl createQueue(QueueRepositoryImpl qRep, String
    // name, EnvProperties envProps,
    // boolean popLocks, boolean distributed) throws Exception {
    // CassQueueFactoryImpl cqf = new CassQueueFactoryImpl(qRep);
    // CassQueueImpl cq =
    // cqf.createQueueInstance(name, envProps.getMaxPushTimeOfPipe(),
    // envProps.getMaxPushesPerPipe(), false);
    // return cq;
    // }

    public static List<PushPopAbstractBase> startPushers(CassQueueImpl cq, EnvProperties envProps) {
        List<PushPopAbstractBase> retList = new ArrayList<PushPopAbstractBase>(envProps.getNumPushers());
        WorkerThreadWatcher ptw = new PusherThreadWatcher(envProps, cq, retList);
        ptw.start();
        return retList;
    }

    public static List<PushPopAbstractBase> startPoppers(CassQueueImpl cq, Queue<CassQMsg> popQ, EnvProperties envProps) {
        List<PushPopAbstractBase> retList = new ArrayList<PushPopAbstractBase>(envProps.getNumPoppers());
        WorkerThreadWatcher ptw = new PopperThreadWatcher(envProps, cq, retList, popQ);
        ptw.start();
        return retList;
    }

    static abstract class WorkerThreadWatcher implements Runnable {
        protected EnvProperties envProps;
        protected List<PushPopAbstractBase> workerList;
        protected CassQueueImpl cq;

        private Thread theThread;

        public WorkerThreadWatcher(EnvProperties envProps, CassQueueImpl cq, List<PushPopAbstractBase> workerList) {
            this.envProps = envProps;
            this.cq = cq;
            this.workerList = workerList;
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
                    try {
                        adjustWorkers();
                    }
                    catch (Exception e) {
                        logger.error("exception while adjusting workers", e);
                    }
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

        private void adjustWorkers() throws Exception {
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

        protected abstract void addWorker() throws Exception;
    }

    static class PusherThreadWatcher extends WorkerThreadWatcher {
        private int pusherId = 0;
        private AtomicLong numGen = new AtomicLong();

        public PusherThreadWatcher(EnvProperties envProps, CassQueueImpl cq, List<PushPopAbstractBase> workerList) {
            super(envProps, cq, workerList);
        }

        @Override
        protected void addWorker() throws IOException {
            CassQueuePusher cqPusher = new CassQueuePusher(pusherId++, cq, numGen, envProps);
            workerList.add(cqPusher);
            cqPusher.start(envProps.getNumMsgs() / envProps.getNumPushers());
        }

        @Override
        protected int getTargetSize() {
            return envProps.getNumPushers();
        }
    }

    static class PopperThreadWatcher extends WorkerThreadWatcher {
        private Queue<CassQMsg> popQ;
        private int popperId = 0;

        public PopperThreadWatcher(EnvProperties envProps, CassQueueImpl cq, List<PushPopAbstractBase> workerList,
                Queue<CassQMsg> popQ) {
            super(envProps, cq, workerList);
            this.popQ = popQ;
        }

        @Override
        protected void addWorker() throws Exception {
            CassQueuePopper cqPopper = new CassQueuePopper(popperId++, cq, envProps, popQ);
            workerList.add(cqPopper);
            cqPopper.start(envProps.getNumMsgs() / envProps.getNumPoppers());

        }

        @Override
        protected int getTargetSize() {
            return envProps.getNumPoppers();
        }
    }
}
