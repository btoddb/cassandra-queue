package com.real.cassandra.queue;

import java.util.UUID;

import me.prettyprint.cassandra.model.HColumn;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.utils.UUIDGen;
import org.scale7.cassandra.pelops.Bytes;

import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.utils.MyInetAddress;

public class CassQMsgFactory {
    private static MyInetAddress inetAddr = new MyInetAddress();

    public CassQMsg createInstance(PipeDescriptorImpl pipeDesc, UUID msgId, String msgData) {
        return new CassQMsg(pipeDesc, msgId, msgData);
    }

    public CassQMsg createInstance(PipeDescriptorImpl pipeDesc, String msgData) {
        return createInstance(pipeDesc, createMsgId(), msgData);
    }

    public CassQMsg createInstance(PipeDescriptorImpl pipeDesc, Column col) {
        return createInstance(pipeDesc, Bytes.fromBytes(col.getName()).toUuid(), new String(col.getValue()));
    }

    public UUID createMsgId() {
        return UUIDGen.makeType1UUIDFromHost(inetAddr.get());
    }

    public CassQMsg createInstance(PipeDescriptorImpl pipeDesc, HColumn<UUID, byte[]> hColumn) {
        return createInstance(pipeDesc, hColumn.getName(), new String(hColumn.getValue()));
    }
}
