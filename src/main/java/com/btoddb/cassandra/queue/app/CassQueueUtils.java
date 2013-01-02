package com.btoddb.cassandra.queue.app;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.cassandra.queue.CassQMsg;
import com.btoddb.cassandra.queue.CassQueueImpl;

public class CassQueueUtils {
    private static Logger logger = LoggerFactory.getLogger(CassQueueUtils.class);

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
                if (null != valueSet && (valueSet.contains(qMsg) || !valueSet.add(new String(qMsg.getMsgDesc().getPayload())))) {
                    fail("value of message pushed more than once : " + qMsg.toString());
                }
                logger.debug("everything OK!");
            }
            else {
                try {
                    Thread.sleep(500);
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

    public static WorkerThreadWatcher startPushers(CassQueueImpl cq, QueueProperties envProps) {
        WorkerThreadWatcher ptw = new PusherThreadWatcher(envProps, cq);
        ptw.start();
        return ptw;
    }

    public static WorkerThreadWatcher startPoppers(CassQueueImpl cq, Queue<CassQMsg> popQ, QueueProperties envProps) {
        WorkerThreadWatcher ptw = new PopperThreadWatcher(envProps, cq, popQ);
        ptw.start();
        return ptw;
    }

    static class PusherThreadWatcher extends WorkerThreadWatcher {
        private int pusherId = 0;
        private AtomicLong numGen = new AtomicLong();

        public PusherThreadWatcher(QueueProperties envProps, CassQueueImpl cq) {
            super(envProps, cq);
        }

        @Override
        protected void addWorker() throws IOException {
            CassQueuePusher cqPusher = new CassQueuePusher(envProps.getTesterId(), pusherId++, cq, numGen, envProps);
            workerList.add(cqPusher);
            cqPusher.start(envProps.getNumMsgs() / envProps.getNumPushers(), pusherId);
        }

        @Override
        protected int getTargetSize() {
            return envProps.getNumPushers();
        }
    }

    static class PopperThreadWatcher extends WorkerThreadWatcher {
        private Queue<CassQMsg> popQ;
        private int popperId = 0;

        public PopperThreadWatcher(QueueProperties envProps, CassQueueImpl cq, Queue<CassQMsg> popQ) {
            super(envProps, cq);
            this.popQ = popQ;
        }

        @Override
        protected void addWorker() throws Exception {
            CassQueuePopper cqPopper = new CassQueuePopper(popperId++, cq, envProps, popQ);
            workerList.add(cqPopper);
            cqPopper.start(envProps.getNumMsgs() / envProps.getNumPoppers(), popperId);

        }

        @Override
        protected int getTargetSize() {
            return envProps.getNumPoppers();
        }
    }
}
