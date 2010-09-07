package com.real.cassandra.queue.pipeperpusher;

import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.scale7.cassandra.pelops.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueDescriptorFactory {
    private static Logger logger = LoggerFactory.getLogger(QueueDescriptorFactory.class);

    public QueueDescriptor createInstance(String qName, List<Column> colList) {
        QueueDescriptor qDesc = new QueueDescriptor(qName);
        for (Column col : colList) {
            if (QueueRepositoryImpl.QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE.equals(Bytes.fromBytes(col.getName()))) {
                qDesc.setMaxPushTimeOfPipe(Bytes.fromBytes(col.getValue()).toLong());
            }
            else if (QueueRepositoryImpl.QDESC_COLNAME_MAX_PUSHES_PER_PIPE.equals(Bytes.fromBytes(col.getName()))) {
                qDesc.setMaxPushesPerPipe(Bytes.fromBytes(col.getValue()).toInt());
            }
            else if (QueueRepositoryImpl.QDESC_COLNAME_MAX_POP_WIDTH.equals(Bytes.fromBytes(col.getName()))) {
                qDesc.setMaxPopWidth(Bytes.fromBytes(col.getValue()).toInt());
            }
            else if (QueueRepositoryImpl.QDESC_COLNAME_POP_PIPE_REFRESH_DELAY.equals(Bytes.fromBytes(col.getName()))) {
                qDesc.setPopPipeRefreshDelay(Bytes.fromBytes(col.getValue()).toInt());
            }
            else {
                logger.warn("unknown queue attribute found in database, ignoring : " + new String(col.getName()));
            }
        }

        return qDesc;
    }

    public QueueDescriptor createInstance(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe) {
        QueueDescriptor qDesc = new QueueDescriptor(qName);
        qDesc.setMaxPushTimeOfPipe(maxPushTimeOfPipe);
        qDesc.setMaxPushesPerPipe(maxPushesPerPipe);
        return qDesc;
    }
}
