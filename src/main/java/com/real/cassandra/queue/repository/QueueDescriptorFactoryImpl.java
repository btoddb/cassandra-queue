package com.real.cassandra.queue.repository;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;

import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.utils.IntegerSerializer;

public class QueueDescriptorFactoryImpl {

    public QueueDescriptor createInstance(String qName, long maxPushTimeOfPipe, int maxPushesPerPipe, int maxPopWidth, long popPipeRefreshDelay) {
        QueueDescriptor qDesc = new QueueDescriptor(qName);
        qDesc.setMaxPushTimeOfPipe(maxPushTimeOfPipe);
        qDesc.setMaxPushesPerPipe(maxPushesPerPipe);
        qDesc.setMaxPopWidth(maxPopWidth);
        qDesc.setPopPipeRefreshDelay(popPipeRefreshDelay);
        return qDesc;
    }

    public QueueDescriptor createInstance(String qName, ColumnSlice<String, byte[]> colSlice) {
        if (colSlice.getColumns().isEmpty()) {
            return null;
        }

        QueueDescriptor qDesc = new QueueDescriptor(qName);
        qDesc.setMaxPopWidth(IntegerSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.QDESC_COLNAME_MAX_POP_WIDTH).getValue()));
        qDesc.setMaxPushesPerPipe(IntegerSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.QDESC_COLNAME_MAX_PUSHES_PER_PIPE).getValue()));
        qDesc.setMaxPushTimeOfPipe(LongSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE).getValue()));
        qDesc.setPopPipeRefreshDelay(LongSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.QDESC_COLNAME_POP_PIPE_REFRESH_DELAY).getValue()));
        return qDesc;
    }

}
