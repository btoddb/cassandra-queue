package com.real.cassandra.queue;

import java.util.HashSet;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.locks.LocalLockerImpl;
import com.real.cassandra.queue.pipes.PipeDescriptorFactory;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.utils.JmxMBeanManager;
import com.real.cassandra.queue.utils.RollingStat;

public class CassQueueImpl implements CassQueueMXBean {
    private static Logger logger = LoggerFactory.getLogger(CassQueueImpl.class);

    private String qName;
    private long maxPushTimePerPipe;
    private int maxPushesPerPipe;
    private int maxPopWidth;
    private QueueRepositoryImpl qRepos;
    private PipeDescriptorFactory pipeDescFactory;
    private LocalLockerImpl popLocker;
    private long popPipeRefreshDelay;

    private PusherImpl rollbackPusher;

    private Set<PusherImpl> pusherSet = new HashSet<PusherImpl>();
    private Set<PopperImpl> popperSet = new HashSet<PopperImpl>();

    private RollingStat popNotEmptyStat = new RollingStat(60000);
    private RollingStat popEmptyStat = new RollingStat(60000);
    private RollingStat pushStat = new RollingStat(60000);
    private PipeReaper pipeReaper;

    public CassQueueImpl(QueueRepositoryImpl qRepos, PipeDescriptorFactory pipeDescFactory, String qName,
            long maxPushTimePerPipe, int maxPushesPerPipe, int popWidth, LocalLockerImpl popLocker,
            LocalLockerImpl queueStatsLocker, long popPipeRefreshDelay) {
        this.qName = qName;
        this.qRepos = qRepos;
        this.pipeDescFactory = pipeDescFactory;
        this.maxPushTimePerPipe = maxPushTimePerPipe;
        this.maxPushesPerPipe = maxPushesPerPipe;
        this.maxPopWidth = popWidth;
        this.popLocker = popLocker;
        this.popPipeRefreshDelay = popPipeRefreshDelay;

        logger.debug("creating pusher for rollback only");
        this.rollbackPusher = createPusher();

        this.pipeReaper = new PipeReaper(qName, qRepos, queueStatsLocker);
        this.pipeReaper.start();

        initJmx();
    }

    private void initJmx() {
        String beanName = JMX_MBEAN_OBJ_NAME_PREFIX + qName;
        try {
            JmxMBeanManager.getInstance().registerMBean(this, beanName);
        }
        catch (InstanceAlreadyExistsException e1) {
            logger.warn("exception while registering MBean, " + beanName + " - ignoring");
        }
        catch (Exception e) {
            throw new RuntimeException("exception while registering MBean, " + beanName);
        }
    }

    public void commit(CassQMsg qMsg) {
        logger.debug("commit {}", qMsg);
        qRepos.removeMsgFromPendingPipe(qMsg);
    }

    public CassQMsg rollback(CassQMsg qMsg) {
        logger.debug("rollback {}", qMsg);
        CassQMsg qNewMsg = rollbackPusher.push(qMsg.getMsgData());
        qRepos.removeMsgFromPendingPipe(qMsg);
        return qNewMsg;
    }

    public PusherImpl createPusher() {
        logger.debug("creating pusher for queue {}", qName);
        PusherImpl pusher = new PusherImpl(this, qRepos, pipeDescFactory, pipeReaper, pushStat);
        pusherSet.add(pusher);
        return pusher;
    }

    public PopperImpl createPopper(boolean startPipeWatcher) {
        logger.debug("creating popper for queue {}", qName);
        PopperImpl popper = new PopperImpl(this, qRepos, pipeReaper, popLocker, popNotEmptyStat, popEmptyStat);
        popperSet.add(popper);
        popper.initialize(startPipeWatcher);
        return popper;
    }

    public void drop() throws Exception {
        logger.debug("dropping queue {}", qName);
        qRepos.dropQueue(this);
    }

    public void truncate() throws Exception {
        logger.debug("truncating queue {}", qName);
        qRepos.truncateQueueData(this);
    }

    @Override
    public long getMaxPushTimePerPipe() {
        return maxPushTimePerPipe;
    }

    @Override
    public void setMaxPushTimePerPipe(long maxPushTimeOfPipe) {
        this.maxPushTimePerPipe = maxPushTimeOfPipe;
    }

    @Override
    public int getMaxPushesPerPipe() {
        return maxPushesPerPipe;
    }

    @Override
    public void setMaxPushesPerPipe(int maxPushesPerPipe) {
        this.maxPushesPerPipe = maxPushesPerPipe;
    }

    @Override
    public int getMaxPopWidth() {
        return maxPopWidth;
    }

    @Override
    public void setMaxPopWidth(int maxPopWidth) {
        this.maxPopWidth = maxPopWidth;
    }

    @Override
    public String getName() {
        return qName;
    }

    public void forcePipeReaperWakeUp() {
        pipeReaper.wakeUp();
    }

    public void shutdown() {
        pipeReaper.shutdown();
        popLocker.shutdown();
    }

    public long getPopPipeRefreshDelay() {
        return popPipeRefreshDelay;
    }

    public void setPopPipeRefreshDelay(long popPipeRefreshDelay) {
        this.popPipeRefreshDelay = popPipeRefreshDelay;
    }

    @Override
    public long getPopCountLocalNotEmpty() {
        return popNotEmptyStat.getTotalSamplesProcessed();
    }

    @Override
    public long getPopCountLocalEmpty() {
        return popEmptyStat.getTotalSamplesProcessed();
    }

    @Override
    public long getPopCountCluster() {
        QueueStats qStats = qRepos.getQueueStats(getName());
        return qStats.getTotalPops();
    }

    @Override
    public double getPopAvgTimeLocal_NotEmpty() {
        return popNotEmptyStat.getAvgOfValues();
    }

    @Override
    public double getPopPerSecondLocal_NotEmpty() {
        return popNotEmptyStat.getSamplesPerSecond();
    }

    @Override
    public long getPushCountLocal() {
        return pushStat.getTotalSamplesProcessed();
    }

    @Override
    public long getPushCountCluster() {
        QueueStats qStats = qRepos.getQueueStats(getName());
        return qStats.getTotalPushes();
    }

    @Override
    public double getPushAvgTimeLocal() {
        return pushStat.getAvgOfValues();
    }

    @Override
    public double getPushPerSecondLocal() {
        return pushStat.getSamplesPerSecond();
    }

    @Override
    public int getNumPoppers() {
        return popperSet.size();
    }

    @Override
    public int getNumPushers() {
        return pusherSet.size();
    }

    public void setPipeReaperProcessingDelay(long delay) {
        pipeReaper.setProcessingDelay(delay);
    }
}
