package com.real.cassandra.queue.pipeperpusher;

public class CassQueueFactoryImpl {
    private QueueRepositoryImpl qRepos;
    private PipeDescriptorFactory pipeDescFactory;
    private PipeLockerImpl popLocker = new PipeLockerImpl();

    public CassQueueFactoryImpl(QueueRepositoryImpl qRepos, PipeDescriptorFactory pipeDescFactory) {
        this.qRepos = qRepos;
        this.pipeDescFactory = pipeDescFactory;
    }

    public CassQueueImpl createQueueInstance(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe,
            int maxPopWidth, boolean distributed) throws Exception {
        QueueDescriptor qDesc =
                qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe, maxPopWidth);
        CassQueueImpl cq =
                new CassQueueImpl(qRepos, pipeDescFactory, qName, qDesc.getMaxPushTimeOfPipe(),
                        qDesc.getMaxPushesPerPipe(), maxPopWidth, popLocker);
        return cq;
    }

}
