package com.real.cassandra.queue.pipeperpusher;

public class CassQueueFactoryImpl {
    private QueueRepositoryImpl qRepos;
    private PipeDescriptorFactory pipeDescFactory;

    public CassQueueFactoryImpl(QueueRepositoryImpl qRepos, PipeDescriptorFactory pipeDescFactory) {
        this.qRepos = qRepos;
        this.pipeDescFactory = pipeDescFactory;
    }

    public CassQueueImpl createQueueInstance(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe,
            boolean distributed) throws Exception {
        QueueDescriptor qDesc = qRepos.createQueueIfDoesntExist(qName, maxPushTimeOfPipe, maxPushesPerPipe);
        CassQueueImpl cq = new CassQueueImpl(qName, qDesc.getMaxPushTimeOfPipe(), qDesc.getMaxPushesPerPipe());
        return cq;
    }

    public PopperImpl createPopper(CassQueueImpl cq) {
        // TODO Auto-generated method stub
        return null;
    }

    public PusherImpl createPusher(CassQueueImpl cq) {
        PusherImpl pusher = new PusherImpl(cq, qRepos, pipeDescFactory);
        return pusher;
    }

}
