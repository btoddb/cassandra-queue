package com.real.cassandra.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraQueueChannelAdapter {
    private static Logger logger = LoggerFactory.getLogger(CassandraQueueChannelAdapter.class);

    // public static final long WAIT_FOREVER = -1L;
    // public static final long WAIT_NEVER = 0L;

    private CassQueue cq;

    // private long checkDelay = 500; // millis

    public CassandraQueueChannelAdapter(CassQueue cq) {
        this.cq = cq;
    }

    public Event pop() {
        Event evt;
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

    // public Event pop(long timeout) {
    // long start = System.currentTimeMillis();
    // long tmpDelay = checkDelay;
    // for (;;) {
    // //
    // // pop event (if there is one)
    // //
    //
    // Event evt;
    // try {
    // evt = cq.pop();
    // if (null != evt) {
    // cq.commit(evt);
    // }
    // }
    // catch (Exception e) {
    // logger.error("exception while popping from queue", e);
    // return null;
    // }
    //
    // // if got event, we done
    // if (null != evt) {
    // return evt;
    // }
    //
    // //
    // // are we finished
    // // - if timeout indicates try only once, we're done
    // // - if timeout has expired, we're done
    // //
    //
    // boolean timeExpired = timeout < System.currentTimeMillis() - start;
    // if (WAIT_NEVER == timeout || (WAIT_FOREVER != timeout && timeExpired)) {
    // return null;
    // }
    //
    // //
    // // wait before retry
    // //
    //
    // if (0 < timeout && tmpDelay > timeout) {
    // tmpDelay = timeout;
    // }
    //
    // try {
    // Thread.sleep(tmpDelay);
    // }
    // catch (InterruptedException e) {
    // logger.error("exception while Thread.sleep", e);
    // }
    // }
    // }

    public void push(String data) {
        try {
            cq.push(data);
        }
        catch (Exception e) {
            logger.error("exception while pushing onto queue", e);
        }
    }

    // private Message<String> convertEventToMessage(Event evt) {
    // return new EventAsSiMsg(evt);
    // }

}
