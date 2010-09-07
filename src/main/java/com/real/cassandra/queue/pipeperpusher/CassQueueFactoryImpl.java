package com.real.cassandra.queue.pipeperpusher;

public class CassQueueFactoryImpl {
    private QueueRepositoryImpl qRepos;
    private PipeDescriptorFactory pipeDescFactory;
    private PipeLockerImpl popLocker = null;

    public CassQueueFactoryImpl(QueueRepositoryImpl qRepos, PipeDescriptorFactory pipeDescFactory,
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
        CassQueueImpl cq =
                new CassQueueImpl(qRepos, pipeDescFactory, qName, qDesc.getMaxPushTimeOfPipe(),
                        qDesc.getMaxPushesPerPipe(), qDesc.getMaxPopWidth(), popLocker, qDesc.getPopPipeRefreshDelay());
        return cq;
    }

}
