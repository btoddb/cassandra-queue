package com.real.cassandra.queue.app;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueImpl;

public class CassQueueUtils {
    private static Logger logger = LoggerFactory.getLogger(CassQueueUtils.class);

    private static boolean cassandraStarted = false;

    public static final String QUEUE_POOL_NAME = "myTestPool";
    public static final String SYSTEM_POOL_NAME = "mySystemPool";
    public static final String QUEUE_NAME = "myTestQueue";
    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    public static boolean monitorPushersPoppers(Queue<CassQMsg> popQ, List<PushPopAbstractBase> pusherSet,
            List<PushPopAbstractBase> popperSet, Set<CassQMsg> msgSet, Set<String> valueSet) {
        //
        // process popped messages and wait until finished - make sure a message
        // is only processed once
        //

        logger.info("start monitoring for end-of-test conditions");

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

    public static String formatMsgValue(String base, int pipeNum) {
        return base + "-" + pipeNum;
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
        if (cassandraStarted) {
            return;
        }

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

        cassandraStarted = true;

        Thread t = new Thread(cassandra);
        t.setName(cassandra.getClass().getSimpleName());
        t.setDaemon(true);
        t.start();
        logger.info("setup: started embedded cassandra thread");
        Thread.sleep(1000);
    }

    public static QueueProperties createEnvPropertiesWithDefaults() {
        Properties rawProps = new Properties();
        rawProps.setProperty(QueueProperties.ENV_numPipes, "4");
        rawProps.setProperty(QueueProperties.ENV_hosts, "localhost");
        rawProps.setProperty(QueueProperties.ENV_RPC_PORT, "9161");
        rawProps.setProperty(QueueProperties.ENV_maxActive, "16");
        rawProps.setProperty(QueueProperties.ENV_maxIdle, "16");
        rawProps.setProperty(QueueProperties.ENV_REPLICATION_FACTOR, "1");
        rawProps.setProperty(QueueProperties.ENV_dropKeyspace, "true");
        return new QueueProperties(rawProps);
    }

    public static List<PushPopAbstractBase> startPushers(CassQueueImpl cq, QueueProperties envProps) {
        List<PushPopAbstractBase> retList = new ArrayList<PushPopAbstractBase>(envProps.getNumPushers());
        WorkerThreadWatcher ptw = new PusherThreadWatcher(envProps, cq, retList);
        ptw.start();
        return retList;
    }

    public static List<PushPopAbstractBase> startPoppers(CassQueueImpl cq, Queue<CassQMsg> popQ, QueueProperties envProps) {
        List<PushPopAbstractBase> retList = new ArrayList<PushPopAbstractBase>(envProps.getNumPoppers());
        WorkerThreadWatcher ptw = new PopperThreadWatcher(envProps, cq, retList, popQ);
        ptw.start();
        return retList;
    }

    static abstract class WorkerThreadWatcher implements Runnable {
        protected QueueProperties envProps;
        protected List<PushPopAbstractBase> workerList;
        protected CassQueueImpl cq;

        private Thread theThread;

        public WorkerThreadWatcher(QueueProperties envProps, CassQueueImpl cq, List<PushPopAbstractBase> workerList) {
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

        public PusherThreadWatcher(QueueProperties envProps, CassQueueImpl cq, List<PushPopAbstractBase> workerList) {
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

        public PopperThreadWatcher(QueueProperties envProps, CassQueueImpl cq, List<PushPopAbstractBase> workerList,
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
