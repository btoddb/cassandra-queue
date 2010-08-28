package com.real.cassandra.queue;

import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.scale7.cassandra.pelops.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.repository.QueueRepository;

public class QueueDescriptorFactory {
    private static Logger logger = LoggerFactory.getLogger(QueueDescriptorFactory.class);

    public QueueDescriptor createInstance(String qName, List<Column> colList) {

        QueueDescriptor qDesc = new QueueDescriptor(qName);
        for (Column col : colList) {
            if (QueueRepository.NUM_PIPES_COL_NAME.equals(Bytes.fromBytes(col.getName()))) {
                qDesc.setNumPipes(Bytes.fromBytes(col.getValue()).toInt());
            }
            else if (QueueRepository.PUSH_PIPE_COL_NAME.equals(Bytes.fromBytes(col.getName()))) {
                qDesc.setPushStartPipe(Bytes.fromBytes(col.getValue()).toLong());
            }
            else if (QueueRepository.POP_PIPE_COL_NAME.equals(Bytes.fromBytes(col.getName()))) {
                qDesc.setPopStartPipe(Bytes.fromBytes(col.getValue()).toLong());
            }
            else {
                logger.warn("unknown queue attribute found in database, ignoring : "
                        + Bytes.fromBytes(col.getName()).toString());
            }
        }

        return qDesc;
    }
}
