package com.real.cassandra.queue.zookeeper;

/**
 * @author Andrew Ebaugh
 */
public class ZooKeeperException extends Exception {

    public ZooKeeperException(String message) {
        super(message);
    }

    public ZooKeeperException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZooKeeperException(Throwable cause) {
        super(cause);
    }
}
