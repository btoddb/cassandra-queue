package com.real.cassandra.queue.app;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueueFactoryImpl;
import com.real.cassandra.queue.CassQueueImpl;
import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueStats;
import com.real.cassandra.queue.locks.LocalLockerImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.pipes.PipeStatus;
import com.real.cassandra.queue.repository.HectorUtils;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;

/**
 * Command line tool used for dumping queues, pipes, counts, etc. Run w/o
 * arguments to get usage.
 * 
 * @author Todd Burruss
 */
public class CassQueueApp {
    private static Logger logger = LoggerFactory.getLogger(CassQueueApp.class);

    private static final String OPT_COUNT = "count";
    private static final String OPT_DUMP_PIPE = "dump-pipe";
    private static final String OPT_OLDEST_PIPES = "list-oldest-pipes";
    private static final String OPT_DUMP_QUEUE = "dump-queue";
    private static final String OPT_QUEUE_STATS = "show-stats";

    private static CassQueueFactoryImpl cqFactory;
    private static QueueRepositoryImpl qRepos;
    private static CassQueueImpl cq;

    private static String qName;
    private static String host;
    private static int port;
    private static int replicationFactor = 1;
    private static int maxPushesPerPipe = 5000;

    public static void main(String[] args) throws Exception {
        try {
            CommandLineParser parser = new GnuParser();
            Options options = new Options();

            options.addOption("c", OPT_COUNT, false, "count msgs in queue");
            options.addOption("p", OPT_DUMP_PIPE, true, "dump contents of pipe");
            options.addOption("o", OPT_OLDEST_PIPES, false, "dump UUIDs of oldest pipes");
            options.addOption("q", OPT_DUMP_QUEUE, false, "dump queue messages and string values");
            options.addOption("s", OPT_QUEUE_STATS, false, "show queue stats");

            CommandLine cmdLine;
            try {
                cmdLine = parser.parse(options, args);
            }
            catch (UnrecognizedOptionException e) {
                System.out.println();
                System.out.println(e.getMessage());

                showUsage();
                return;
            }
            if (3 > cmdLine.getArgList().size()) {
                showUsage();
                return;
            }

            @SuppressWarnings("unchecked")
            List<String> argsList = cmdLine.getArgList();
            qName = argsList.get(0);
            host = argsList.get(1);
            port = Integer.parseInt(argsList.get(2));
            maxPushesPerPipe = 5000;

            System.out.println();
            System.out.println("qName : " + qName);
            System.out.println("host  : " + host);
            System.out.println("port  : " + port);
            System.out.println();

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
            else if (cmdLine.hasOption(OPT_QUEUE_STATS)) {
                showQueueStats();
            }
            else {
                logger.error("missing cmd parameter");
            }
        }
        finally {
            shutdownQueueMgrAndPool();
        }
    }

    private static void showQueueStats() {
        QueueStats qStats = qRepos.getQueueStats(qName);
        System.out.println("qStats = " + qStats);
    }

    private static void showUsage() {
        System.out.println();

        System.out.println("usage:  " + CassQueueApp.class.getSimpleName() + " <options> <queue-name> <host> <port>");
        System.out.println();
        System.out.println("   options:");
        System.out.println();
        System.out.println("   --" + OPT_DUMP_QUEUE + " : dump contents of the specified queue");
        System.out.println("   --" + OPT_DUMP_PIPE + " <pipe-id> : dump contents of pipe (pipe-id is a UUID)");
        System.out.println("   --" + OPT_OLDEST_PIPES + " : list oldest pipe descriptors in queue");
        System.out.println("   --" + OPT_COUNT + " : count number of msgs 'waiting' in queue");
        System.out.println("   --" + OPT_QUEUE_STATS + " : show stats for specified queue");
        System.out.println();

        System.out.println();
    }

    private static void dumpQueue(CommandLine cmdLine) {
        List<CassQMsg> result = qRepos.getOldestMsgsFromQueue(qName, 100);
        for (CassQMsg qMsg : result) {
            System.out.println(qMsg.toString());
        }
    }

    private static void dumpPipe(CommandLine cmdLine) throws Exception {
        String pipeIdAsStr = cmdLine.getOptionValue(OPT_DUMP_PIPE);
        System.out.println("pipeId = " + pipeIdAsStr);
        PipeDescriptorImpl pipeDesc = qRepos.getPipeDescriptor(UUID.fromString(pipeIdAsStr));
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
        msgList = qRepos.getPendingMessagesFromPipe(pipeDesc, maxPushesPerPipe + 1);
        if (msgList.isEmpty()) {
            System.out.println("pending: <pipe is empty>");
        }
        for (CassQMsg qMsg : msgList) {
            System.out.println("pending msg : " + qMsg.toString());
        }
    }

    private static void dumpOldestPipes(CommandLine cmdLine) throws Exception {
        System.out.println("<pipe descriptor> = <number of waiting msgs>");
        System.out.println();
        List<PipeDescriptorImpl> pdList = qRepos.getOldestPopActivePipes(qName, 100);
        for (PipeDescriptorImpl pipeDesc : pdList) {
            System.out.println(pipeDesc.toString() + " = "
                    + qRepos.getWaitingMessagesFromPipe(pipeDesc, maxPushesPerPipe).size());
        }
    }

    private static void countMsgs() throws Exception {
        QueueRepositoryImpl.CountResult waitingCount = qRepos.getCountOfWaitingMsgs(qName, maxPushesPerPipe);
        QueueRepositoryImpl.CountResult pendingCount = qRepos.getCountOfPendingCommitMsgs(qName, maxPushesPerPipe);

        System.out.println("waiting : " + waitingCount.totalMsgCount);
        System.out.println("pending : " + pendingCount.totalMsgCount);

        System.out.println("  --- push status counts");
        for (Entry<PipeStatus, Integer> entry : waitingCount.pushStatusCounts.entrySet()) {
            System.out.println("pipes with push status, " + entry.getKey() + " = " + entry.getValue());
        }
        System.out.println("  --- pop status counts");
        for (Entry<PipeStatus, Integer> entry : waitingCount.popStatusCounts.entrySet()) {
            System.out.println("pipes with pop status, " + entry.getKey() + " = " + entry.getValue());
        }
    }

    private static void setupQueueSystem() throws Exception {
        Properties rawProps = new Properties();
        rawProps.put(QueueProperties.ENV_hosts, host);
        rawProps.put(QueueProperties.ENV_RPC_PORT, String.valueOf(port));
        rawProps.put(QueueProperties.ENV_REPLICATION_FACTOR, replicationFactor);
        rawProps.put(QueueProperties.ENV_TRANSACTION_TIMEOUT, 30000);
        QueueProperties envProps = new QueueProperties(rawProps);

        qRepos = HectorUtils.createQueueRepository(envProps);
        cqFactory = new CassQueueFactoryImpl(qRepos, new LocalLockerImpl<QueueDescriptor>(), new LocalLockerImpl<QueueDescriptor>());
        cq = cqFactory.createInstance(qName, false);
    }

    private static void shutdownQueueMgrAndPool() {
        if (null != cq) {
            cq.shutdownAndWait();
        }
    }

}
