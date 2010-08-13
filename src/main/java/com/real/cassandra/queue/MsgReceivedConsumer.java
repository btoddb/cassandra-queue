package com.real.cassandra.queue;

import java.util.LinkedList;
import java.util.Queue;

import org.springframework.integration.Message;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.stereotype.Service;

@Service("msgReceivedConsumer")
public class MsgReceivedConsumer {
    private Queue<Event> msgQueue = new LinkedList<Event>();

    @ServiceActivator
    public void execute(Message<Event> msg) {
        Event evt = msg.getPayload();
        msgQueue.offer(evt);
    }

    public void clear() {
        msgQueue.clear();
    }

    public Queue<Event> getMsgQueue() {
        return msgQueue;
    }
}
