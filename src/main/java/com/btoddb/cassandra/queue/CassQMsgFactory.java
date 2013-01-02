package com.btoddb.cassandra.queue;

import java.util.UUID;

import com.btoddb.cassandra.queue.utils.UuidGenerator;

public class CassQMsgFactory {

    public UUID createMsgId() {
        return UuidGenerator.generateTimeUuid();
    }
}
