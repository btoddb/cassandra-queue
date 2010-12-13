package com.real.cassandra.queue;

import com.real.cassandra.queue.locks.Locker;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;

public class CassQueueFactoryImpl {
    private QueueRepositoryImpl qRepos;
    private Locker<QueueDescriptor> queueStatsLocker;
    private Locker<QueueDescriptor> pipeCollectionLocker;

    public CassQueueFactoryImpl(QueueRepositoryImpl qRepos, Locker<QueueDescriptor> queueStatsLocker,
            Locker<QueueDescriptor> pipeCollectionLocker) {
        this.qRepos = qRepos;
        this.queueStatsLocker = queueStatsLocker;
        this.pipeCollectionLocker = pipeCollectionLocker;
    }

    public CassQueueImpl createInstance(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe, long transactionTimeout, boolean distributed) {
        QueueDescriptor qDesc = qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, transactionTimeout);
        CassQueueImpl cq = new CassQueueImpl(qRepos, qDesc, true, queueStatsLocker, pipeCollectionLocker);
        return cq;
    }

    public CassQueueImpl createInstance(String qName) throws Exception {
        return createInstance(qName, true);
    }

    public CassQueueImpl createInstance(String qName, boolean startReaper) throws Exception {
        QueueDescriptor qDesc = qRepos.getQueueDescriptor(qName);
        if (null != qDesc) {
            return new CassQueueImpl(qRepos, qDesc, startReaper, queueStatsLocker, pipeCollectionLocker);
        }
        else {
            return null;
        }
    }

}
