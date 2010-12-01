package com.real.cassandra.queue.utils;

import java.util.UUID;

import me.prettyprint.cassandra.serializers.UUIDSerializer;

import org.apache.cassandra.utils.UUIDGen;

public class UuidGenerator {
    public static UUID generateTimeUuid() {
        return UUIDGen.makeType1UUIDFromHost(MyIp.get());
    }

    public static UUID createInstance(byte[] raw) {
        return UUIDSerializer.get().fromBytes(raw);
    }
}
