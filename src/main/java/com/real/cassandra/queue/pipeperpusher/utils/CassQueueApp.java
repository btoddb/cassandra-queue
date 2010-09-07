package com.real.cassandra.queue.pipeperpusher.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.Pelops;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.pipeperpusher.CassQueueFactoryImpl;
import com.real.cassandra.queue.pipeperpusher.CassQueueImpl;
import com.real.cassandra.queue.pipeperpusher.PipeDescriptorFactory;
import com.real.cassandra.queue.pipeperpusher.PipeLockerImpl;
import com.real.cassandra.queue.pipeperpusher.QueueRepositoryImpl;

public class CassQueueApp {
    private static Logger logger = LoggerFactory.getLogger(CassQueueApp.class);

    private static CassQueueFactoryImpl cqFactory;
    private static QueueRepositoryImpl qRepos;
    private static EnvProperties envProps;
    private static CassQueueImpl cq;

    public static void main(String[] args) throws Exception {
        parseAppProperties();
        setupQueueSystem();

        System.out.println("qName     : " + envProps.getQName());

        QueueRepositoryImpl.CountResult waitingCount = qRepos.getCountOfWaitingMsgs(envProps.getQName());
        QueueRepositoryImpl.CountResult deliveredCount = qRepos.getCountOfDeliveredMsgs(envProps.getQName());

        System.out.println("waiting   : " + waitingCount.totalMsgCount);
        System.out.println("delivered : " + deliveredCount.totalMsgCount);

        System.out.println("  ---");
        for (Entry<String, Integer> entry : waitingCount.statusCounts.entrySet()) {
            System.out.println("status " + entry.getKey() + " = " + entry.getValue());
        }

        shutdownQueueMgrAndPool();
    }

    private static void parseAppProperties() throws FileNotFoundException, IOException {
        File appPropsFile = new File("conf/app.properties");
        Properties props = new Properties();
        props.load(new FileReader(appPropsFile));
        envProps = new EnvProperties(props);

        logger.info("using hosts : " + props.getProperty("hosts"));
        logger.info("using thrift port : " + props.getProperty("thriftPort"));
    }

    private static void setupQueueSystem() throws Exception {
        qRepos = CassQueueUtils.createQueueRepository(envProps, ConsistencyLevel.QUORUM);
        cqFactory = new CassQueueFactoryImpl(qRepos, new PipeDescriptorFactory(qRepos), new PipeLockerImpl());
        cq = cqFactory.createInstance(envProps.getQName());
    }

    private static void shutdownQueueMgrAndPool() {
        cq.shutdown();
        Pelops.shutdown();
    }

}
