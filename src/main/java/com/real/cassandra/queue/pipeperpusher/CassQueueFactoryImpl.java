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
                new CassQueueImpl(qName, qDesc.getMaxPushTimeOfPipe(), qDesc.getMaxPushesPerPipe(), maxPopWidth);
        return cq;
    }

    public PopperImpl createPopper(CassQueueImpl cq) throws Exception {
        PopperImpl popper = new PopperImpl(cq, qRepos, popLocker);
        return popper;
    }

    public PusherImpl createPusher(CassQueueImpl cq) {
        PusherImpl pusher = new PusherImpl(cq, qRepos, pipeDescFactory);
        return pusher;
    }

}
