package com.real.cassandra.queue.pipeperpusher;

public class CassQueueException extends RuntimeException {

    public CassQueueException(String msg) {
        super(msg);
    }

    public CassQueueException(String msg, Exception e) {
        super(msg, e);
    }

}
