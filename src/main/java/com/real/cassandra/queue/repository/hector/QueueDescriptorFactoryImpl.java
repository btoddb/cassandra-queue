package com.real.cassandra.queue.repository.hector;

import me.prettyprint.cassandra.model.ColumnSlice;
import me.prettyprint.cassandra.model.Result;

import com.real.cassandra.queue.QueueDescriptor;
import com.real.cassandra.queue.QueueDescriptorFactoryAbstractImpl;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;

public class QueueDescriptorFactoryImpl extends QueueDescriptorFactoryAbstractImpl {

    public QueueDescriptor createInstance(String qName, Result<ColumnSlice<String, String>> result) {
        QueueDescriptor qDesc = new QueueDescriptor(qName);
        // qDesc.setMaxPopWidth(result.get().getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_MAX_POP_WIDTH)
        // .getValue());
        // qDesc.setMaxPushesPerPipe(result.get()
        // .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_MAX_PUSHES_PER_PIPE).getValue());
        // qDesc.setMaxPushTimeOfPipe(result.get()
        // .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_MAX_PUSH_TIME_OF_PIPE).getValue());
        // qDesc.setPopPipeRefreshDelay(result.get()
        // .getColumnByName(QueueRepositoryAbstractImpl.QDESC_COLNAME_POP_PIPE_REFRESH_DELAY).getValue());
        return qDesc;
    }
}
