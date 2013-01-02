package com.real.cassandra.queue;

import java.util.UUID;

import com.real.cassandra.queue.utils.UuidGenerator;

public class CassQMsgFactory {

    public UUID createMsgId() {
        return UuidGenerator.generateTimeUuid();
    }
}
