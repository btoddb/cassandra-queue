package com.real.cassandra.queue.repository.hector;

import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueDescriptorFactoryAbstractImpl;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
import com.real.cassandra.queue.utils.IntegerSerializer;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;

public class QueueDescriptorFactoryImpl extends QueueDescriptorFactoryAbstractImpl {

    public QueueDescriptor createInstance(String qName, ColumnSlice<String, byte[]> colSlice) {
        LongSerializer le = LongSerializer.get();
        IntegerSerializer ie = IntegerSerializer.get();

        if (colSlice.getColumns().isEmpty()) {
            return null;
        }

        QueueDescriptor qDesc = new QueueDescriptor(qName);
        qDesc.setMaxPopWidth(ie.fromBytes(colSlice
                .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_MAX_POP_WIDTH).getValue()));
        qDesc.setMaxPushesPerPipe(ie.fromBytes(colSlice
                .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_MAX_PUSHES_PER_PIPE).getValue()));
        qDesc.setMaxPushTimeOfPipe(le.fromBytes(colSlice
                .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE).getValue()));
        qDesc.setPopPipeRefreshDelay(le.fromBytes(colSlice
                .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_POP_PIPE_REFRESH_DELAY).getValue()));
        return qDesc;
    }

}
