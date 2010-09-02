package com.real.cassandra.queue.pipeperpusher;

import java.util.UUID;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.utils.UUIDGen;
import org.scale7.cassandra.pelops.Bytes;

import com.real.cassandra.queue.CassQMsg;

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
}
