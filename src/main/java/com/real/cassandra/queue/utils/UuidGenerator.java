package com.real.cassandra.queue.utils;

import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;

public class UuidGenerator {
    public static UUID generateTimeUuid() {
        return UUIDGen.makeType1UUIDFromHost(MyIp.get());
    }

    public static UUID createInstance(byte[] raw) {
        return UUIDGen.makeType1UUID(raw);
    }
}
