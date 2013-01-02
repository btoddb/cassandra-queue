package com.btoddb.cassandra.queue.utils;

import java.nio.ByteBuffer;

/**
 * Converts bytes to Double and vise a versa
 * 
 * @author Todd Burruss
 * 
 */
public final class DoubleSerializer {
    private static final int BUF_SIZE = 8;
    private static final DoubleSerializer instance = new DoubleSerializer();

    public static DoubleSerializer get() {
        return instance;
    }

    public byte[] toBytes(Double obj) {
        ByteBuffer bb = ByteBuffer.allocate(BUF_SIZE);
        bb.putDouble(obj);
        return bb.array();
    }

    public Double fromBytes(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocate(BUF_SIZE);
        bb.put(bytes);
        return bb.getDouble(0);
    }

}
