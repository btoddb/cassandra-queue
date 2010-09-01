package com.real.cassandra.queue.roundrobin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.EnvProperties;
import com.real.cassandra.queue.QueueDescriptor;

public class PipeSelectionRoundRobinStrategy {
    private static Logger logger = LoggerFactory.getLogger(PipeSelectionRoundRobinStrategy.class);

    private Object pushPipePickMonitorObj = new Object();
    private Object pushPipeIncMonitorObj = new Object();
    private long nextPushPipeToUse = 0;
    private long nextPopPipeOffset = 0;
    private PipeWatcher pipeWatcher;
    private QueueDescriptor qDesc;

    private QueueRepositoryImpl qRepos;
    private EnvProperties envProps;
    private String qName;
    private PipeManagerImpl pipeMgr;

    public PipeSelectionRoundRobinStrategy(EnvProperties envProps, String qName, PipeManagerImpl pipeMgr,
            QueueRepositoryImpl qRepos) {
        this.envProps = envProps;
        this.qName = qName;
        this.pipeMgr = pipeMgr;
        this.qRepos = qRepos;

        pipeWatcher = new PipeWatcher();
        pipeWatcher.start();

    }

    private QueueDescriptor getQueueDescriptor() {
        try {
            return qRepos.getQueueDescriptor(qName);
        }
        catch (Exception e) {
            throw new RuntimeException("exception while picking pop pipe", e);
        }
    }

    public void initQueue() throws Exception {
        QueueDescriptor qDesc = qRepos.getQueueDescriptor(qName);

        long startPipe = qDesc.getPopStartPipe() - 1;
        startPipe = 0 <= startPipe ? startPipe : 0;
        long endPipe = qDesc.getPushStartPipe() + envProps.getNumPipes();

        for (Long pipeNum = startPipe; pipeNum <= endPipe; pipeNum++) {
            pipeMgr.addPipe(pipeNum);
        }
    }

    public void truncate() {
        pipeMgr.truncate();

    }

    public int getCount() throws Exception {
        return qRepos.getCount(qName);
    }

    public PipeDescriptorImpl pickPopPipe() {
        QueueDescriptor qDesc = getQueueDescriptor();
        for (;;) {
            PipeDescriptorImpl pipeDesc;
            pipeDesc = pipeMgr.getPipeDescriptor(nextPopPipeOffset + qDesc.getPopStartPipe());
            // we do the numPipes+1 to give overlap if the pushers have
            // already incremented their start pipe.
            nextPopPipeOffset = (nextPopPipeOffset + 1) % (envProps.getNumPipes() + 1);

            if (pipeMgr.lockPopPipe(pipeDesc)) {
                logger.debug("locked pipe : " + pipeDesc);
                return pipeDesc;
            }
        }
    }

    public void releasePopPipe(PipeDescriptorImpl pipeNum) {
        pipeMgr.releasePopPipe(pipeNum);
    }

    public void popPipeEmpty(PipeDescriptorImpl pipeDesc) {
        QueueDescriptor qDesc = getQueueDescriptor();
        if (Long.valueOf(pipeDesc.getPipeId()) < qDesc.getPushStartPipe() - 1) {
            try {
                incrementPopStartPipe(qDesc);
            }
            catch (Exception e) {
                logger.error("exception while incrementing pop pipe", e);
            }
        }
    }

    private void incrementPopStartPipe(QueueDescriptor qDesc) throws Exception {
        long tmp = qDesc.getPopStartPipe() + 1;
        qRepos.setPopStartPipe(qName, tmp);
        qDesc.setPopStartPipe(tmp);
        pipeMgr.removePipe(pipeMgr.getPipeDescriptor(tmp - 1));
    }

    public PipeDescriptorImpl pickPushPipe() throws Exception {
        synchronized (pushPipePickMonitorObj) {
            long ret = nextPushPipeToUse + qDesc.getPushStartPipe();
            nextPushPipeToUse = (nextPushPipeToUse + 1) % envProps.getNumPipes();
            return pipeMgr.getPipeDescriptor(ret);
        }
    }

    public void releasePushPipe(PipeDescriptorImpl pipeNum) {
        // do nothing
    }

    private void incrementPushStartPipe() throws Exception {
        synchronized (pushPipeIncMonitorObj) {
            long tmp = qDesc.getPushStartPipe() + 1;
            pipeMgr.addPipe(tmp + envProps.getNumPipes());
            qRepos.setPushStartPipe(qName, tmp);
            // don't update qDesc until after the map is updated
            qDesc.setPushStartPipe(tmp);
        }
    }

    public void shutdown() {
        pipeWatcher.setContinueProcessing(false);
    }

    /**
     * Determines when to increase the start pipe for pushing new messages.
     * 
     * @author Todd Burruss
     */
    class PipeWatcher implements Runnable {
        private long sleepTime = 100;
        private long lastPushPipeInctime = 0;
        private boolean continueProcessing = true;
        private Thread theThread;

        public void start() {
            theThread = new Thread(pipeWatcher);
            theThread.setName(getClass().getSimpleName());
            theThread.setDaemon(true);
            theThread.start();
        }

        @Override
        public void run() {
            lastPushPipeInctime = System.currentTimeMillis();
            while (continueProcessing) {
                try {
                    qDesc = qRepos.getQueueDescriptor(qName);
                    if (System.currentTimeMillis() - lastPushPipeInctime > envProps.getPushPipeIncrementDelay()) {
                        try {
                            incrementPushStartPipe();
                            // read again to get latest data
                            qDesc = qRepos.getQueueDescriptor(qName);
                            lastPushPipeInctime = System.currentTimeMillis();
                        }
                        catch (Throwable e) {
                            logger.error("exception while incrementing push start pipe", e);
                        }
                    }

                }
                catch (Exception e) {
                    logger.error("exception while getting queue descriptor", e);
                }

                try {
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        }

        public boolean isContinueProcessing() {
            return continueProcessing;
        }

        public void setContinueProcessing(boolean continueProcessing) {
            this.continueProcessing = continueProcessing;
            theThread.interrupt();
        }
    }

    public long getPushStartPipe() {
        return qDesc.getPushStartPipe();
    }

    public long getPopStartPipe() {
        return qDesc.getPopStartPipe();
    }

}
