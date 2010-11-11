package com.real.cassandra.queue.pipes;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.real.cassandra.queue.repository.QueueRepositoryImpl;

import me.prettyprint.cassandra.serializers.BytesSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

public class PipeDescriptorFactory {

    public PipeDescriptorImpl createInstance(String qName, UUID pipeId) {
        return new PipeDescriptorImpl(qName, pipeId);
    }

    public Set<HColumn<String, byte[]>> createInstance(PipeDescriptorImpl pipeDesc) {
        Set<HColumn<String, byte[]>> colSet = new HashSet<HColumn<String, byte[]>>();

        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_QUEUE_NAME,
                StringSerializer.get().toBytes(pipeDesc.getQName()), StringSerializer.get(), BytesSerializer.get()));
        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_PUSH_STATUS,
                StringSerializer.get().toBytes(pipeDesc.getPushStatus().getName()), StringSerializer.get(),
                BytesSerializer.get()));
        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_POP_STATUS,
                StringSerializer.get().toBytes(pipeDesc.getPopStatus().getName()), StringSerializer.get(),
                BytesSerializer.get()));
        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_PUSH_COUNT,
                IntegerSerializer.get().toBytes(pipeDesc.getPushCount()), StringSerializer.get(), BytesSerializer.get()));
        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_POP_COUNT,
                IntegerSerializer.get().toBytes(pipeDesc.getPopCount()), StringSerializer.get(), BytesSerializer.get()));
        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_START_TIMESTAMP, LongSerializer.get()
                .toBytes(pipeDesc.getStartTimestamp()), StringSerializer.get(), BytesSerializer.get()));
        return colSet;
    }

    public PipeDescriptorImpl createInstance(UUID pipeId, ColumnSlice<String, byte[]> colSlice) {
        if (colSlice.getColumns().isEmpty()) {
            return null;
        }

        if(colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_QUEUE_NAME) == null) {
            System.out.println("Null col slice");
        }
        PipeDescriptorImpl pipeDesc =
                new PipeDescriptorImpl(StringSerializer.get().fromBytes(
                        colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_QUEUE_NAME).getValue()), pipeId);
        pipeDesc.setPushStatus(PipeStatus.getInstance(StringSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_PUSH_STATUS).getValue())));
        pipeDesc.setPopStatus(PipeStatus.getInstance(StringSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_POP_STATUS).getValue())));

        pipeDesc.setPushCount(IntegerSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_PUSH_COUNT).getValue()));
        pipeDesc.setPopCount(IntegerSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_POP_COUNT).getValue()));
        pipeDesc.setStartTimestamp(LongSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_START_TIMESTAMP).getValue()));
        return pipeDesc;
    }
}
