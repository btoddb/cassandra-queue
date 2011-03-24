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

import com.hazelcast.core.Hazelcast;
import com.real.cassandra.queue.app.CassQueueUtils;
import com.real.cassandra.queue.app.PushPopAbstractBase;
import com.real.cassandra.queue.app.QueueProperties;
import com.real.cassandra.queue.app.WorkerThreadWatcher;
import com.real.cassandra.queue.locks.Locker;
import com.real.cassandra.queue.locks.hazelcast.HazelcastLockerImpl;
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

//        String ZK_CONNECT_STRING = "kv-app07.dev.real.com:2181,kv-app08.dev.real.com:2181,kv-app09.dev.real.com:2181";
//        queueStatsLocker = new ZooKeeperLockerImpl<QueueDescriptor>("/queue-stats", ZK_CONNECT_STRING, 6000);
//        pipeCollectionLocker = new ZooKeeperLockerImpl<QueueDescriptor>("/pipes", ZK_CONNECT_STRING, 6000);
        queueStatsLocker = new HazelcastLockerImpl<QueueDescriptor>("queue-stats");
        pipeCollectionLocker = new HazelcastLockerImpl<QueueDescriptor>("pipes");

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

        queueStatsLocker.shutdownAndWait();
        pipeCollectionLocker.shutdownAndWait();

//        System.out.println( "queueStatsLocker : lockCountSuccess          = " + ((HazelcastLockerImpl)queueStatsLocker).getLockCountSuccess());
//        System.out.println( "queueStatsLocker : lockCountFailure          = " + ((HazelcastLockerImpl)queueStatsLocker).getLockCountFailure());
//        System.out.println( "queueStatsLocker : lockCountFailureWithRetry = " + ((HazelcastLockerImpl)queueStatsLocker).getLockCountFailureWithRetry());
//        System.out.println( "queueStatsLocker : releaseCount              = " + ((HazelcastLockerImpl)queueStatsLocker).getReleaseCount());
//        
//        System.out.println();
//        
//        System.out.println( "pipeCollectionLocker : lockCountSuccess          = " + ((HazelcastLockerImpl)pipeCollectionLocker).getLockCountSuccess());
//        System.out.println( "pipeCollectionLocker : lockCountFailure          = " + ((HazelcastLockerImpl)pipeCollectionLocker).getLockCountFailure());
//        System.out.println( "pipeCollectionLocker : lockCountFailureWithRetry = " + ((HazelcastLockerImpl)pipeCollectionLocker).getLockCountFailureWithRetry());
//        System.out.println( "pipeCollectionLocker : releaseCount              = " + ((HazelcastLockerImpl)pipeCollectionLocker).getReleaseCount());
//
//        System.out.println();
        
        System.out.println( "pickNewPipeSuccess = " + cq.getPopperPickNewPipeSuccess() );
        System.out.println( "pickNewPipeFailure = " + cq.getPopperPickNewPipeFailure() );
        System.out.println( "acquireLockFailure = " + cq.getPopperAcquireLockFailure() );
        System.out.println( "rollback count     = " + cq.getRollbackCount());
        Hazelcast.shutdownAll();
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
