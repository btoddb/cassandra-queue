package com.real.cassandra.queue;

import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;

import com.real.cassandra.queue.utils.MyIp;

public class CassQMsgFactory {

    public UUID createMsgId() {
        return UUIDGen.makeType1UUIDFromHost(MyIp.get());
    }
}
