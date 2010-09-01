package com.real.cassandra.queue.pipeperpusher;

import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;

import com.real.cassandra.queue.CassQMsg;

public class CassQMsgFactory {
    private static MyInetAddress inetAddr = new MyInetAddress();

    public CassQMsg createInstance(PipeDescriptorImpl pipeDesc, UUID msgId, String msgData) {
        return new CassQMsg(pipeDesc, msgId, msgData);
    }

    public CassQMsg createInstance(PipeDescriptorImpl pipeDesc, String msgData) {
        return createInstance(pipeDesc, UUIDGen.makeType1UUIDFromHost(inetAddr.get()), msgData);
    }
}
