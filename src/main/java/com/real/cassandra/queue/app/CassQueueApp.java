package com.real.cassandra.queue.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.scale7.cassandra.pelops.Pelops;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueFactoryImpl;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorFactory;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeLockerImpl;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
import com.real.cassandra.queue.repository.RepositoryFactoryImpl;
import com.real.cassandra.queue.repository.pelops.QueueRepositoryImpl;

public class CassQueueApp {
    private static Logger logger = LoggerFactory.getLogger(CassQueueApp.class);

    private static final String OPT_COUNT = "count";
    private static final String OPT_DUMP_PIPE = "dump-pipe";
    private static final String OPT_OLDEST_PIPES = "oldest-pipes";

    private static CassQueueFactoryImpl cqFactory;
    private static QueueRepositoryAbstractImpl qRepos;
    private static EnvProperties envProps;
    private static CassQueueImpl cq;

    public static void main(String[] args) throws Exception {
        parseAppProperties();
        setupQueueSystem();

        System.out.println("qName     : " + envProps.getQName());

        CommandLineParser parser = new GnuParser();
        Options options = new Options();

        options.addOption("c", OPT_COUNT, false, "count msgs in queue");
        options.addOption("p", OPT_DUMP_PIPE, true, "dump contents of pipe");
        options.addOption("o", OPT_OLDEST_PIPES, false, "dump UUIDs of oldest pipes");

        CommandLine cmdLine = parser.parse(options, args);
        if (cmdLine.hasOption(OPT_COUNT)) {
            countMsgs();
        }
        else if (cmdLine.hasOption(OPT_DUMP_PIPE)) {
            dumpPipe(cmdLine);
        }
        else if (cmdLine.hasOption(OPT_OLDEST_PIPES)) {
            dumpOldestPipes(cmdLine);
        }
        else {
            logger.error("missing cmd parameter");
        }

        shutdownQueueMgrAndPool();
    }

    private static void dumpPipe(CommandLine cmdLine) throws Exception {
        String pipeIdAsStr = cmdLine.getOptionValue(OPT_DUMP_PIPE);
        PipeDescriptorImpl pipeDesc = qRepos.getPipeDescriptor(envProps.getQName(), UUID.fromString(pipeIdAsStr));
        outputPipeDescription(pipeDesc);
    }

    private static void outputPipeDescription(PipeDescriptorImpl pipeDesc) throws Exception {
        System.out.println("pipeId : " + pipeDesc.getPipeId() + " : " + pipeDesc.getStatus() + ", "
                + pipeDesc.getMsgCount());
        List<CassQMsg> msgList = qRepos.getWaitingMessagesFromPipe(pipeDesc, envProps.getMaxPushesPerPipe() + 1);
        if (msgList.isEmpty()) {
            System.out.println("<pipe is empty>");
        }
        for (CassQMsg qMsg : msgList) {
            System.out.println("msg : " + qMsg.toString());
        }
    }

    private static void dumpOldestPipes(CommandLine cmdLine) throws Exception {
        List<PipeDescriptorImpl> pdList = qRepos.getOldestNonEmptyPipes(envProps.getQName(), envProps.getMaxPopWidth());
        for (PipeDescriptorImpl pipeDesc : pdList) {
            System.out.println(pipeDesc.toString() + " = "
                    + qRepos.getWaitingMessagesFromPipe(pipeDesc, envProps.getMaxPushesPerPipe()).size());
        }
    }

    private static void countMsgs() throws Exception {
        QueueRepositoryImpl.CountResult waitingCount = qRepos.getCountOfWaitingMsgs(envProps.getQName());
        QueueRepositoryImpl.CountResult deliveredCount = qRepos.getCountOfPendingCommitMsgs(envProps.getQName());

        System.out.println("waiting   : " + waitingCount.totalMsgCount);
        System.out.println("delivered : " + deliveredCount.totalMsgCount);

        System.out.println("  ---");
        for (Entry<String, Integer> entry : waitingCount.statusCounts.entrySet()) {
            System.out.println("status " + entry.getKey() + " = " + entry.getValue());
        }
    }

    private static void parseAppProperties() throws FileNotFoundException, IOException {
        File appPropsFile = new File("conf/app.properties");
        Properties props = new Properties();
        props.load(new FileReader(appPropsFile));
        envProps = new EnvProperties(props);

        logger.info("using hosts : " + envProps.getHostArr());
        logger.info("using thrift port : " + envProps.getRpcPort());
    }

    private static void setupQueueSystem() throws Exception {
        qRepos = new RepositoryFactoryImpl().createInstance(envProps, ConsistencyLevel.QUORUM);
        cqFactory = new CassQueueFactoryImpl(qRepos, new PipeDescriptorFactory(qRepos), new PipeLockerImpl());
        cq = cqFactory.createInstance(envProps.getQName());
    }

    private static void shutdownQueueMgrAndPool() {
        cq.shutdown();
        Pelops.shutdown();
    }

}
