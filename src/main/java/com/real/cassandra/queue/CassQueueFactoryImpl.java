package com.real.cassandra.queue;

import com.real.cassandra.queue.locks.Locker;
import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.repository.QueueRepositoryImpl;

public class CassQueueFactoryImpl {
    private QueueRepositoryImpl qRepos;
    private Locker<PipeDescriptorImpl> popLocker;
    private Locker<QueueDescriptor> queueStatsLocker;

    public CassQueueFactoryImpl(QueueRepositoryImpl qRepos, Locker<PipeDescriptorImpl> popLocker, Locker<QueueDescriptor> queueStatsLocker) {
        this.qRepos = qRepos;
        this.popLocker = popLocker;
        this.queueStatsLocker = queueStatsLocker;
    }

    public CassQueueImpl createInstance(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe, int maxPopWidth,
            long popPipeRefreshDelay, boolean distributed) {
        QueueDescriptor qDesc =
                qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);
        CassQueueImpl cq =
                new CassQueueImpl(qRepos, qDesc, true, maxPopWidth, popLocker, queueStatsLocker, popPipeRefreshDelay);
        return cq;
    }

    public CassQueueImpl createInstance(String qName) throws Exception {
        return createInstance(qName, true);
    }

    public CassQueueImpl createInstance(String qName, boolean startReaper) throws Exception {
        QueueDescriptor qDesc = qRepos.getQueueDescriptor(qName);
        if (null != qDesc) {
            return new CassQueueImpl(qRepos, qDesc, startReaper, qDesc.getMaxPopWidth(), popLocker, queueStatsLocker,
                    qDesc.getPopPipeRefreshDelay());
        }
        else {
            return null;
        }
    }

}
