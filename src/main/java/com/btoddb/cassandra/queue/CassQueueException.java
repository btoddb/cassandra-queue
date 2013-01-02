package com.btoddb.cassandra.queue;

public class CassQueueException extends RuntimeException {
    private static final long serialVersionUID = -775441436709947561L;

    public CassQueueException(String msg) {
        super(msg);
    }

    public CassQueueException(String msg, Throwable e) {
        super(msg, e);
    }

}
