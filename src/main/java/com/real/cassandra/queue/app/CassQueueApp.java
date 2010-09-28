package com.real.cassandra.queue.app;

import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueFactoryImpl;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorFactory;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeLockerImpl;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl.CountResult;
import com.real.cassandra.queue.repository.hector.HectorUtils;
import com.real.cassandra.queue.repository.hector.QueueRepositoryImpl;

public class CassQueueApp {
    private static Logger logger = LoggerFactory.getLogger(CassQueueApp.class);

    private static final String OPT_COUNT = "count";
    private static final String OPT_DUMP_PIPE = "dump-pipe";
    private static final String OPT_OLDEST_PIPES = "dump-oldest-pipes";
    private static final String OPT_DUMP_QUEUE = "dump-queue";

    private static CassQueueFactoryImpl cqFactory;
    private static QueueRepositoryImpl qRepos;
    // private static EnvProperties envProps;
    private static CassQueueImpl cq;

    private static String qName;
    private static String host;
    private static int port = 9160;
    private static int replicationFactor = 1;
    private static int maxPushesPerPipe = 5000;
    private static int maxPopWidth = 4;

    public static void main(String[] args) throws Exception {
        try {
            CommandLineParser parser = new GnuParser();
            Options options = new Options();

            options.addOption("c", OPT_COUNT, false, "count msgs in queue");
            options.addOption("p", OPT_DUMP_PIPE, true, "dump contents of pipe");
            options.addOption("o", OPT_OLDEST_PIPES, false, "dump UUIDs of oldest pipes");
            options.addOption("q", OPT_DUMP_QUEUE, false, "dump queue messages and string values");

            CommandLine cmdLine = parser.parse(options, args);

            if (2 > cmdLine.getArgList().size()) {
                logger.error("requires <qname> and <host>");
                return;
            }

            List<String> argsList = cmdLine.getArgList();
            qName = argsList.get(0);
            host = argsList.get(1);
            maxPushesPerPipe = 5000;

            System.out.println("qName : " + qName);
            System.out.println("host  : " + host);

            // parseAppProperties();
            setupQueueSystem();

            if (null == cq) {
                System.out.println("Queue, " + qName + ", has not been created");
            }
            else if (cmdLine.hasOption(OPT_COUNT)) {
                countMsgs();
            }
            else if (cmdLine.hasOption(OPT_DUMP_PIPE)) {
                dumpPipe(cmdLine);
            }
            else if (cmdLine.hasOption(OPT_OLDEST_PIPES)) {
                dumpOldestPipes(cmdLine);
            }
            else if (cmdLine.hasOption(OPT_DUMP_QUEUE)) {
                dumpQueue(cmdLine);
            }
            else {
                logger.error("missing cmd parameter");
            }
        }
        finally {
            shutdownQueueMgrAndPool();
        }
    }

    private static void dumpQueue(CommandLine cmdLine) {
        List<CassQMsg> result = qRepos.getOldestMsgsFromQueue(qName, 100);
        for (CassQMsg qMsg : result) {
            System.out.println(qMsg.toString());
        }
    }

    private static void dumpPipe(CommandLine cmdLine) throws Exception {
        String pipeIdAsStr = cmdLine.getOptionValue(OPT_DUMP_PIPE);
        PipeDescriptorImpl pipeDesc = qRepos.getPipeDescriptor(qName, UUID.fromString(pipeIdAsStr));
        outputPipeDescription(qRepos, pipeDesc, maxPushesPerPipe);
    }

    public static void outputPipeDescription(QueueRepositoryImpl qRepos, PipeDescriptorImpl pipeDesc,
            int maxPushesPerPipe) throws Exception {
        System.out.println(pipeDesc.toString());
        List<CassQMsg> msgList = qRepos.getWaitingMessagesFromPipe(pipeDesc, maxPushesPerPipe + 1);
        if (msgList.isEmpty()) {
            System.out.println("waiting: <pipe is empty>");
        }
        for (CassQMsg qMsg : msgList) {
            System.out.println("waiting msg : " + qMsg.toString());
        }
        msgList = qRepos.getDeliveredMessagesFromPipe(pipeDesc, maxPushesPerPipe + 1);
        if (msgList.isEmpty()) {
            System.out.println("pending: <pipe is empty>");
        }
        for (CassQMsg qMsg : msgList) {
            System.out.println("pending msg : " + qMsg.toString());
        }
    }

    private static void dumpOldestPipes(CommandLine cmdLine) throws Exception {
        List<PipeDescriptorImpl> pdList = qRepos.getOldestNonEmptyPipes(qName, maxPopWidth);
        for (PipeDescriptorImpl pipeDesc : pdList) {
            System.out.println(pipeDesc.toString() + " = "
                    + qRepos.getWaitingMessagesFromPipe(pipeDesc, maxPushesPerPipe).size());
        }
    }

    private static void countMsgs() throws Exception {
        QueueRepositoryImpl.CountResult waitingCount = qRepos.getCountOfWaitingMsgs(qName);
        QueueRepositoryImpl.CountResult deliveredCount = qRepos.getCountOfPendingCommitMsgs(qName);

        System.out.println("waiting   : " + waitingCount.totalMsgCount);
        System.out.println("delivered : " + deliveredCount.totalMsgCount);

        System.out.println("  ---");
        for (Entry<String, Integer> entry : waitingCount.statusCounts.entrySet()) {
            System.out.println("status " + entry.getKey() + " = " + entry.getValue());
        }
    }

    private static void setupQueueSystem() throws Exception {
        qRepos = HectorUtils.createQueueRepository(new String[] {
            host }, port, replicationFactor, false);
        cqFactory = new CassQueueFactoryImpl(qRepos, new PipeDescriptorFactory(qRepos), new PipeLockerImpl());
        cq = cqFactory.createInstance(qName);
    }

    private static void shutdownQueueMgrAndPool() {
        if (null != cq) {
            cq.shutdown();
        }
    }

}
