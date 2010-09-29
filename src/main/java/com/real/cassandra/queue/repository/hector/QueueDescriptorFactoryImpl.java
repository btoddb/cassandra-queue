package com.real.cassandra.queue.repository.hector;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.query.QueryResult;

import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueDescriptorFactoryAbstractImpl;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
import com.real.cassandra.queue.utils.IntegerSerializer;

public class QueueDescriptorFactoryImpl extends QueueDescriptorFactoryAbstractImpl {

    public QueueDescriptor createInstance(String qName, QueryResult<ColumnSlice<String, byte[]>> result) {
        LongSerializer le = LongSerializer.get();
        IntegerSerializer ie = IntegerSerializer.get();

        ColumnSlice<String, byte[]> colSlice = result.get();
        if (colSlice.getColumns().isEmpty()) {
            return null;
        }

        QueueDescriptor qDesc = new QueueDescriptor(qName);
        qDesc.setMaxPopWidth(ie.fromBytes(result.get()
                .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_MAX_POP_WIDTH).getValue()));
        qDesc.setMaxPushesPerPipe(ie.fromBytes(result.get()
                .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_MAX_PUSHES_PER_PIPE).getValue()));
        qDesc.setMaxPushTimeOfPipe(le.fromBytes(result.get()
                .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE).getValue()));
        qDesc.setPopPipeRefreshDelay(le.fromBytes(result.get()
                .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_POP_PIPE_REFRESH_DELAY).getValue()));
        return qDesc;
    }

}
