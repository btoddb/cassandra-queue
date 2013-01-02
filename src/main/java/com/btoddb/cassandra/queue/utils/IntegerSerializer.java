package com.btoddb.cassandra.queue.utils;

import java.nio.ByteBuffer;

/**
 * Converts bytes to Integer and vise a versa
 * 
 * @author Todd Burruss
 * 
 */
public final class IntegerSerializer {
    private static final int BUF_SIZE = 4;
    private static final IntegerSerializer instance = new IntegerSerializer();

    public static IntegerSerializer get() {
        return instance;
    }

    public byte[] toBytes(Integer obj) {
        ByteBuffer bb = ByteBuffer.allocate(BUF_SIZE);
        bb.putInt(obj);
        return bb.array();
    }

    public Integer fromBytes(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocate(BUF_SIZE);
        bb.put(bytes);
        return bb.getInt(0);
    }

}
