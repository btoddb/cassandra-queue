package com.real.cassandra.queue;

public interface CassQueue {

    /**
     * Truncate all data in the queue.
     * 
     * @throws Exception
     */
    void truncate() throws Exception;

    /**
     * Return name of queue.
     * 
     * @return
     */
    String getName();

}
