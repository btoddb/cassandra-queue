package com.real.cassandra.queue;

import com.real.cassandra.queue.pipes.PipeDescriptorFactory;
import com.real.cassandra.queue.pipes.PipeLockerImpl;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;

public class CassQueueFactoryImpl {
    private QueueRepositoryAbstractImpl qRepos;
    private PipeDescriptorFactory pipeDescFactory;
    private PipeLockerImpl popLocker = null;

    public CassQueueFactoryImpl(QueueRepositoryAbstractImpl qRepos, PipeDescriptorFactory pipeDescFactory,
            PipeLockerImpl popLocker) {
        this.qRepos = qRepos;
        this.pipeDescFactory = pipeDescFactory;
        this.popLocker = popLocker;
    }

    public CassQueueImpl createInstance(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe, int maxPopWidth,
            long popPipeRefreshDelay, boolean distributed) throws Exception {
        QueueDescriptor qDesc =
                qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth, 1000);
        CassQueueImpl cq =
                new CassQueueImpl(qRepos, pipeDescFactory, qName, qDesc.getMaxPushTimeOfPipe(),
                        qDesc.getMaxPushesPerPipe(), maxPopWidth, popLocker, popPipeRefreshDelay);
        return cq;
    }

    public CassQueueImpl createInstance(String qName) throws Exception {
        QueueDescriptor qDesc = qRepos.getQueueDescriptor(qName);
        if (null != qDesc) {
            return new CassQueueImpl(qRepos, pipeDescFactory, qName, qDesc.getMaxPushTimeOfPipe(),
                    qDesc.getMaxPushesPerPipe(), qDesc.getMaxPopWidth(), popLocker, qDesc.getPopPipeRefreshDelay());
        }
        else {
            return null;
        }
    }

}
