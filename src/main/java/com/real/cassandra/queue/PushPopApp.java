package com.real.cassandra.queue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.app.CassQueueUtils;
import com.real.cassandra.queue.app.PushPopAbstractBase;
import com.real.cassandra.queue.app.QueueProperties;
import com.real.cassandra.queue.app.WorkerThreadWatcher;
import com.real.cassandra.queue.locks.CagesLockerImpl;
import com.real.cassandra.queue.locks.Locker;
import com.real.cassandra.queue.repository.HectorUtils;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;

/**
 * Unit tests for {@link CassQueueImpl}.
 * 
 * @author Todd Burruss
 */
public class PushPopApp {
    private static Logger logger = LoggerFactory.getLogger(PushPopApp.class);

    private static CassQueueFactoryImpl cqFactory;
    private static QueueRepositoryImpl qRepos;
    private static QueueProperties envProps;
    private static CassQueueImpl cq;
    private static Locker<QueueDescriptor> queueStatsLocker;
    private static Locker<QueueDescriptor> pipeCollectionLocker;

    public static void main(String[] args) throws Exception {
        logger.info("setting up app properties");
        parseAppProperties();

        String ZK_CONNECT_STRING = "kv-app07.dev.real.com:2181,kv-app08.dev.real.com:2181,kv-app09.dev.real.com:2181";
        queueStatsLocker = new CagesLockerImpl<QueueDescriptor>("/queue-stats", ZK_CONNECT_STRING, 6000, 30);
        pipeCollectionLocker = new CagesLockerImpl<QueueDescriptor>("/pipes", ZK_CONNECT_STRING, 6000, 30);
        logger.info("setting queuing system");
        setupQueueSystem();

        //
        // start a set of pushers and poppers
        //

        logger.info("starting pushers/poppers after 2 sec pause : " + envProps.getNumPushers() + "/"
                + envProps.getNumPoppers());

        Queue<CassQMsg> popQ = new ConcurrentLinkedQueue<CassQMsg>();
        WorkerThreadWatcher pusherWtw = CassQueueUtils.startPushers(cq, envProps);
        WorkerThreadWatcher popperWtw = CassQueueUtils.startPoppers(cq, popQ, envProps);
        List<PushPopAbstractBase> pusherSet = pusherWtw.getWorkerList();
        List<PushPopAbstractBase> popperSet = popperWtw.getWorkerList();

        CassQueueUtils.monitorPushersPoppers(popQ, pusherSet, popperSet, null, null);

        shutdownQueueMgrAndPool();

        pusherWtw.shutdownAndWait();
        popperWtw.shutdownAndWait();

    }

    // -----------------------

    private static void parseAppProperties() throws FileNotFoundException, IOException {
        File appPropsFile = new File("conf/app.properties");
        Properties props = new Properties();
        props.load(new FileReader(appPropsFile));
        envProps = new QueueProperties(props);

        logger.info("using hosts : " + envProps.getHostArr());
        logger.info("using thrift port : " + envProps.getRpcPort());
    }

    private static void setupQueueSystem() throws Exception {
        qRepos = HectorUtils.createQueueRepository(envProps);
        cqFactory = new CassQueueFactoryImpl(qRepos, queueStatsLocker, pipeCollectionLocker);
        cq =
                cqFactory.createInstance(envProps.getQName(), envProps.getMaxPushTimePerPipe(), envProps
                        .getMaxPushesPerPipe(), envProps.getTransactionTimeout(), false);
        if (envProps.getTruncateQueue()) {
            cq.truncate();
            Thread.sleep(2000);
        }
    }

    private static void shutdownQueueMgrAndPool() {
        cq.shutdownAndWait();
    }

}
