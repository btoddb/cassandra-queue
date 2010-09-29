package com.real.cassandra.queue;

import java.util.UUID;

import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.utils.UUIDGen;

import com.real.cassandra.queue.pipes.PipeDescriptorImpl;
import com.real.cassandra.queue.utils.MyIp;

public class CassQMsgFactory {

    public CassQMsg createInstance(PipeDescriptorImpl pipeDesc, UUID msgId, String msgData) {
        return new CassQMsg(pipeDesc, msgId, msgData);
    }

    public CassQMsg createInstance(PipeDescriptorImpl pipeDesc, String msgData) {
        return createInstance(pipeDesc, createMsgId(), msgData);
    }

    public CassQMsg createInstance(PipeDescriptorImpl pipeDesc, Column col) {
        return createInstance(pipeDesc, UUIDGen.makeType1UUID(col.getName()), new String(col.getValue()));
    }

    public UUID createMsgId() {
        return UUIDGen.makeType1UUIDFromHost(MyIp.get());
    }

    public CassQMsg createInstance(PipeDescriptorImpl pipeDesc, HColumn<UUID, byte[]> hColumn) {
        return createInstance(pipeDesc, hColumn.getName(), new String(hColumn.getValue()));
    }
}
