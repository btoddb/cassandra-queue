package com.real.cassandra.queue.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQMsg;
import com.real.cassandra.queue.CassQueue;

/**
 * Spring channel adapter connecting a spring channel to a {@link CassQueue}.
 * 
 * @see #push(String)
 * @see #pop()
 * 
 * @author Todd Burruss
 */
public class CassandraQueueChannelAdapter {
    private static Logger logger = LoggerFactory.getLogger(CassandraQueueChannelAdapter.class);

    private CassQueue cq;

    // private long checkDelay = 500; // millis

    public CassandraQueueChannelAdapter(CassQueue cq) {
        this.cq = cq;
    }

    /**
     * Called by channel adapter periodically to grab message from queue and
     * deliver via channel.
     * 
     * @return {@link CassQMsg}
     */
    public CassQMsg pop() {
        CassQMsg evt;
        try {
            evt = cq.pop();
            if (null != evt) {
                cq.commit(evt);
            }
            return evt;
        }
        catch (Exception e) {
            logger.error("exception while popping from queue", e);
            return null;
        }
    }

    public void push(String data) {
        try {
            cq.push(data);
        }
        catch (Exception e) {
            logger.error("exception while pushing onto queue", e);
        }
    }
}
