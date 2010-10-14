package com.real.cassandra.queue;

import com.real.cassandra.queue.locks.LocalLockerImpl;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;

public class CassQueueFactoryImpl {
    private QueueRepositoryImpl qRepos;
    private LocalLockerImpl popLocker;
    private LocalLockerImpl queueStatsLocker;

    public CassQueueFactoryImpl(QueueRepositoryImpl qRepos, LocalLockerImpl popLocker, LocalLockerImpl queueStatsLocker) {
        this.qRepos = qRepos;
        this.popLocker = popLocker;
        this.queueStatsLocker = queueStatsLocker;
    }

    public CassQueueImpl createInstance(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe, int maxPopWidth,
            long popPipeRefreshDelay, boolean distributed) {
        QueueDescriptor qDesc =
                qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);
        CassQueueImpl cq =
                new CassQueueImpl(qRepos, qName, qDesc.getMaxPushTimeOfPipe(), qDesc.getMaxPushesPerPipe(),
                        maxPopWidth, popLocker, queueStatsLocker, popPipeRefreshDelay);
        return cq;
    }

    public CassQueueImpl createInstance(String qName) throws Exception {
        QueueDescriptor qDesc = qRepos.getQueueDescriptor(qName);
        if (null != qDesc) {
            return new CassQueueImpl(qRepos, qName, qDesc.getMaxPushTimeOfPipe(), qDesc.getMaxPushesPerPipe(),
                    qDesc.getMaxPopWidth(), popLocker, queueStatsLocker, qDesc.getPopPipeRefreshDelay());
        }
        else {
            return null;
        }
    }

}
