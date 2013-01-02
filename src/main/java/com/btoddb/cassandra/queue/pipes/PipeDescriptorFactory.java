package com.btoddb.cassandra.queue.pipes;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.cassandra.queue.repository.QueueRepositoryImpl;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

public class PipeDescriptorFactory {

    private static Logger logger = LoggerFactory.getLogger(PipeDescriptorFactory.class);

    public PipeDescriptorImpl createInstance(String qName, UUID pipeId) {
        return new PipeDescriptorImpl(qName, pipeId);
    }

    public Set<HColumn<String, byte[]>> createInstance(PipeDescriptorImpl pipeDesc) {
        Set<HColumn<String, byte[]>> colSet = new HashSet<HColumn<String, byte[]>>();

        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_QUEUE_NAME, StringSerializer.get().toBytes(
                pipeDesc.getQName()), StringSerializer.get(), BytesArraySerializer.get()));
        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_PUSH_STATUS, StringSerializer.get().toBytes(
                pipeDesc.getPushStatus().getName()), StringSerializer.get(), BytesArraySerializer.get()));
        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_POP_STATUS, StringSerializer.get().toBytes(
                pipeDesc.getPopStatus().getName()), StringSerializer.get(), BytesArraySerializer.get()));
        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_PUSH_COUNT, IntegerSerializer.get().toBytes(
                pipeDesc.getPushCount()), StringSerializer.get(), BytesArraySerializer.get()));
        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_POP_COUNT, IntegerSerializer.get().toBytes(
                pipeDesc.getPopCount()), StringSerializer.get(), BytesArraySerializer.get()));
        colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_PUSH_START_TIMESTAMP, LongSerializer.get()
                .toBytes(pipeDesc.getPushStartTimestamp()), StringSerializer.get(), BytesArraySerializer.get()));

        if (null != pipeDesc.getPopOwner()) {
            colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_POP_OWNER_ID, UUIDSerializer.get()
                    .toBytes(pipeDesc.getPopOwner()), StringSerializer.get(), BytesArraySerializer.get()));
        }

        if (null != pipeDesc.getPopOwnTimestamp()) {
            colSet.add(HFactory.createColumn(QueueRepositoryImpl.PDESC_COLNAME_POP_OWNER_TIMESTAMP, LongSerializer
                    .get().toBytes(pipeDesc.getPopOwnTimestamp()), StringSerializer.get(), BytesArraySerializer.get()));
        }

        return colSet;
    }

    public PipeDescriptorImpl createInstance(UUID pipeId, ColumnSlice<String, byte[]> colSlice) {
        if (colSlice.getColumns().isEmpty()) {
            return null;
        }

        if (colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_QUEUE_NAME) == null) {
            logger.error("Tried to create a pipe descriptor with no qName. This indicates the pipe was deleted by" +
                    " one thread and updated by another -- likely a synchronization issue;" +
                    " pipe ID: {}", pipeId.toString());
            return null;
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
        pipeDesc.setPushStartTimestamp(LongSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_PUSH_START_TIMESTAMP).getValue()));

        HColumn<String, byte[]> col = colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_POP_OWNER_ID);
        if (null != col) {
            pipeDesc.setPopOwner(UUIDSerializer.get().fromBytes(col.getValue()));
        }

        col = colSlice.getColumnByName(QueueRepositoryImpl.PDESC_COLNAME_POP_OWNER_TIMESTAMP);
        if (null != col) {
            pipeDesc.setPopOwnTimestamp(LongSerializer.get().fromBytes(col.getValue()));
        }
        
        return pipeDesc;
    }
}
