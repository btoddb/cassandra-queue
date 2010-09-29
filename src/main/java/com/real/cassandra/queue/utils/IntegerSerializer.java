package com.real.cassandra.queue.utils;

import java.nio.ByteBuffer;

/**
 * Converts bytes to Long and vise a versa
 * 
 * @author Ran Tavory
 * 
 */
public final class IntegerSerializer {
    private static final int INT_SIZE = 4;
    private static final IntegerSerializer instance = new IntegerSerializer();

    public static IntegerSerializer get() {
        return instance;
    }

    public byte[] toBytes(Integer obj) {
        ByteBuffer bb = ByteBuffer.allocate(INT_SIZE);
        bb.putInt(obj);
        return bb.array();
    }

    public Integer fromBytes(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocate(INT_SIZE);
        bb.put(bytes);
        bb.position(0);
        return bb.getInt();
    }

}
