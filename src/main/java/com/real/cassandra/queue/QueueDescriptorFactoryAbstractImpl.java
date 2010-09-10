package com.real.cassandra.queue;

public abstract class QueueDescriptorFactoryAbstractImpl {
    // private static Logger logger =
    // LoggerFactory.getLogger(QueueDescriptorFactoryAbstractImpl.class);

    public QueueDescriptor createInstance(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe) {
        QueueDescriptor qDesc = new QueueDescriptor(qName);
        qDesc.setMaxPushTimeOfPipe(maxPushTimeOfPipe);
        qDesc.setMaxPushesPerPipe(maxPushesPerPipe);
        return qDesc;
    }
}
