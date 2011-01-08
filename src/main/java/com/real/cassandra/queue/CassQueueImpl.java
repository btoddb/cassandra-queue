package com.real.cassandra.queue;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.management.InstanceAlreadyExistsException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.locks.Locker;
import com.real.cassandra.queue.pipes.PipeManager;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.utils.JmxMBeanManager;
import com.real.cassandra.queue.utils.RollingStat;

/**
 * A client's view of a queue. Since the queue is distributed, each client has
 * its view of the queue. More than likely this view of the queue will always be
 * out of date, but the data and stats are accurate for that moment in time.
 * <p/>
 * This class provides common tasks like truncation, popper/pusher creation, JMX
 * stats, and configuration.
 * <p/>
 * Most operations in the distributed queue rely on all clients' time to be
 * synchronized to a common source. If the clients drift apart more than a
 * second or two, problems can occur such as messages being rolled back because
 * of transaction timeout when they shouldn't, or pipes being closed when they
 * shouldn't.
 * 
 * @author Todd Burruss
 */
public class CassQueueImpl implements CassQueueMXBean {
    private static Logger logger = LoggerFactory.getLogger(CassQueueImpl.class);

    public static final long TRANSACTION_GRACE_PERIOD = 2000;

    private QueueDescriptor qDesc;
    private QueueRepositoryImpl qRepos;

    private Locker<QueueDescriptor> pipeCollectionLocker;
    private Locker<QueueDescriptor> queueStatsLocker;
    private PusherImpl rollbackPusher;
    private long maxPopOwnerIdleTime;

    private Set<PusherImpl> pusherSet = new HashSet<PusherImpl>();
    private Set<PopperImpl> popperSet = new HashSet<PopperImpl>();

    private RollingStat popNotEmptyStat = new RollingStat(60000);
    private RollingStat popEmptyStat = new RollingStat(60000);
    private RollingStat pushStat = new RollingStat(60000);

    private PipeReaper pipeReaper;

    public CassQueueImpl(QueueRepositoryImpl qRepos, QueueDescriptor qDesc, boolean startReaper,
            Locker<QueueDescriptor> queueStatsLocker, Locker<QueueDescriptor> pipeCollectionLocker) {
        this.qDesc = qDesc;
        this.qRepos = qRepos;
        this.queueStatsLocker = queueStatsLocker;
        this.pipeCollectionLocker = pipeCollectionLocker;

        this.maxPopOwnerIdleTime = getTransactionTimeout();
        
        logger.debug("creating pusher for rollback only");
        this.rollbackPusher = createPusher();

        // for unit testing you might not want to start the reaper immediately
        if (startReaper) {
            this.pipeReaper = new PipeReaper(this, qRepos, this.queueStatsLocker);
            this.pipeReaper.start();
        }

        initJmx();
    }

    private void initJmx() {
        String beanName = JMX_MBEAN_OBJ_NAME_PREFIX + qDesc.getName();
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

    /**
     * A message that has been popped is in a "pending" state. If the message is
     * not committed or rolled back before the
     * {@link #getTransactionTimeout(long)} is exceeded, the {@link PipeReaper}
     * will rollback the message.
     * 
     * @param qMsg
     * @return
     */
    public boolean checkTransactionTimeoutExpired(CassQMsg qMsg) {
        return System.currentTimeMillis() - qMsg.getMsgDesc().getPopTimestamp() > (getTransactionTimeout() + TRANSACTION_GRACE_PERIOD);
    }

    public void commit(CassQMsg qMsg) {
        logger.debug("commit {}", qMsg);
        qRepos.removeMsgFromPendingPipe(qMsg);
    }

    public CassQMsg rollback(CassQMsg qMsg) {
        logger.debug("rollback {}", qMsg);
        CassQMsg qNewMsg = rollbackPusher.push(qMsg.getMsgDesc().getPayload());
        qRepos.removeMsgFromPendingPipe(qMsg);
        return qNewMsg;
    }

    public PusherImpl createPusher() {
        logger.debug("creating pusher for queue {}", qDesc.getName());
        PusherImpl pusher = new PusherImpl(this, qRepos, pushStat);
        pusherSet.add(pusher);
        return pusher;
    }

    /**
     * Preferred way to create a popper. Insures all common properties are used
     * to instantiate the popper.
     * 
     * @return An instantiated {@link PopperImpl} if successful.
     */
    public PopperImpl createPopper() {
        logger.debug("creating popper for queue {}", qDesc.getName());
        UUID popperId = UUID.randomUUID();
        PipeManager pipeMgr = new PipeManager(qRepos, this, popperId, pipeCollectionLocker);
        pipeMgr.setMaxOwnerIdleTime(getMaxPopOwnerIdleTime());
        PopperImpl popper = new PopperImpl(popperId, this, qRepos, pipeMgr, popNotEmptyStat, popEmptyStat);
        popperSet.add(popper);
        return popper;
    }

    /**
     * Drop the queue making it no longer available to clients. This method will
     * remove the queue data from cassandra and cause all future push/pops to
     * fail after it is completely removed.
     * 
     * @throws Exception
     */
    public void drop() throws Exception {
        logger.debug("dropping queue {}", qDesc.getName());
        qRepos.dropQueue(this);
    }

    /**
     * Truncate the queue data. The queue will still be available for push/pop,
     * but all data will be removed.
     * 
     * @throws Exception
     */
    public void truncate() throws Exception {
        logger.debug("truncating queue {}", qDesc.getName());
        qRepos.truncateQueueData(this);
        for ( PopperImpl popper : popperSet ) {
            popper.clearPipeManagerSelection();
        }
    }

    @Override
    public long getMaxPushTimePerPipe() {
        return qDesc.getMaxPushTimePerPipe();
    }

    @Override
    public void setMaxPushTimePerPipe(long maxPushTimeOfPipe) {
        qDesc.setMaxPushTimePerPipe(maxPushTimeOfPipe);
    }

    @Override
    public int getMaxPushesPerPipe() {
        return qDesc.getMaxPushesPerPipe();
    }

    @Override
    public void setMaxPushesPerPipe(int maxPushesPerPipe) {
        qDesc.setMaxPushesPerPipe(maxPushesPerPipe);
    }

    @Override
    public String getName() {
        return qDesc.getName();
    }

    /**
     * Force the reaper to process pipes immediately.
     */
    public void forcePipeReaperWakeUp() {
        pipeReaper.wakeUp();
    }

    /**
     * Graceful shutdown of this client's push/pop workers.  This is not required but will free resources
     * shared across all clients.
     */
    public void shutdownAndWait() {
        for (PusherImpl pusher : pusherSet) {
            pusher.shutdownAndWait();
        }

        for (PopperImpl popper : popperSet) {
            popper.shutdownAndWait();
        }

        if (null != pipeReaper) {
            pipeReaper.shutdownAndWait();
        }
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
        QueueStats qStats = qRepos.calculateUpToDateQueueStats(getName());
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
        QueueStats qStats = qRepos.calculateUpToDateQueueStats(getName());
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

    public long getTransactionTimeout() {
        return qDesc.getTransactionTimeout();
    }

    public void setTransactionTimeout(long transactionTimeout) {
        qDesc.setTransactionTimeout(transactionTimeout);
    }

    public QueueDescriptor getQueueDescriptor() {
        return qDesc;
    }

    PipeReaper getPipeReaper() {
        return pipeReaper;
    }

    @Override
    public long getQueueDepth() {
        return getPushCountCluster() - getPopCountCluster();
    }

    public long getMaxPopOwnerIdleTime() {
        return maxPopOwnerIdleTime;
    }

    public void setMaxPopOwnerIdleTime(long maxPopOwnerIdleTime) {
        this.maxPopOwnerIdleTime = maxPopOwnerIdleTime;
    }
}
