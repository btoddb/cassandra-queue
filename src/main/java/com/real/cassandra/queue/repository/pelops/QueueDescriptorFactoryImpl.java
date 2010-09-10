package com.real.cassandra.queue.repository.pelops;

import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.scale7.cassandra.pelops.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueDescriptorFactoryAbstractImpl;

public class QueueDescriptorFactoryImpl extends QueueDescriptorFactoryAbstractImpl {
    private static Logger logger = LoggerFactory.getLogger(QueueDescriptorFactoryImpl.class);

    public QueueDescriptor createInstance(String qName, List<Column> colList) {
        QueueDescriptor qDesc = new QueueDescriptor(qName);
        for (Column col : colList) {
            String colName = new String(col.getName());
            if (QueueRepositoryImpl.QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE.equals(colName)) {
                qDesc.setMaxPushTimeOfPipe(Bytes.fromBytes(col.getValue()).toLong());
            }
            else if (QueueRepositoryImpl.QDESC_COLNAME_MAX_PUSHES_PER_PIPE.equals(colName)) {
                qDesc.setMaxPushesPerPipe(Bytes.fromBytes(col.getValue()).toInt());
            }
            else if (QueueRepositoryImpl.QDESC_COLNAME_MAX_POP_WIDTH.equals(colName)) {
                qDesc.setMaxPopWidth(Bytes.fromBytes(col.getValue()).toInt());
            }
            else if (QueueRepositoryImpl.QDESC_COLNAME_POP_PIPE_REFRESH_DELAY.equals(colName)) {
                qDesc.setPopPipeRefreshDelay(Bytes.fromBytes(col.getValue()).toInt());
            }
            else {
                logger.warn("unknown queue attribute found in database, ignoring : " + colName);
            }
        }

        return qDesc;
    }
}
