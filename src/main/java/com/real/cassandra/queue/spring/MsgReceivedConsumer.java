package com.real.cassandra.queue.spring;

import java.util.LinkedList;
import java.util.Queue;

import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.stereotype.Service;

@Service("msgReceivedConsumer")
public class MsgReceivedConsumer {
    private Queue<String> msgQueue = new LinkedList<String>();

    @ServiceActivator
    public void execute(String msg) {
        msgQueue.offer(msg);
    }

    public void clear() {
        msgQueue.clear();
    }

    public Queue<String> getMsgQueue() {
        return msgQueue;
    }
}
