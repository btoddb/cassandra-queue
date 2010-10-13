package com.real.cassandra.queue.repository;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;

import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.utils.IntegerSerializer;

public class QueueDescriptorFactoryImpl {

    public QueueDescriptor createInstance(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe) {
        QueueDescriptor qDesc = new QueueDescriptor(qName);
        qDesc.setMaxPushTimeOfPipe(maxPushTimeOfPipe);
        qDesc.setMaxPushesPerPipe(maxPushesPerPipe);
        return qDesc;
    }

    public QueueDescriptor createInstance(String qName, ColumnSlice<String, byte[]> colSlice) {
        LongSerializer le = LongSerializer.get();
        IntegerSerializer ie = IntegerSerializer.get();

        if (colSlice.getColumns().isEmpty()) {
            return null;
        }

        QueueDescriptor qDesc = new QueueDescriptor(qName);
        qDesc.setMaxPopWidth(ie.fromBytes(colSlice.getColumnByName(QueueRepositoryImpl.QDESC_COLNAME_MAX_POP_WIDTH)
                .getValue()));
        qDesc.setMaxPushesPerPipe(ie.fromBytes(colSlice.getColumnByName(
                QueueRepositoryImpl.QDESC_COLNAME_MAX_PUSHES_PER_PIPE).getValue()));
        qDesc.setMaxPushTimeOfPipe(le.fromBytes(colSlice.getColumnByName(
                QueueRepositoryImpl.QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE).getValue()));
        qDesc.setPopPipeRefreshDelay(le.fromBytes(colSlice.getColumnByName(
                QueueRepositoryImpl.QDESC_COLNAME_POP_PIPE_REFRESH_DELAY).getValue()));
        return qDesc;
    }

}
