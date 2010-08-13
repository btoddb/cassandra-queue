package com.real.cassandra.queue.spring;

import java.util.LinkedList;
import java.util.Queue;

import org.springframework.integration.Message;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.stereotype.Service;

import com.real.cassandra.queue.CassQMsg;

@Service("msgReceivedConsumer")
public class MsgReceivedConsumer {
    private Queue<CassQMsg> msgQueue = new LinkedList<CassQMsg>();

    @ServiceActivator
    public void execute(Message<CassQMsg> msg) {
        CassQMsg evt = msg.getPayload();
        msgQueue.offer(evt);
    }

    public void clear() {
        msgQueue.clear();
    }

    public Queue<CassQMsg> getMsgQueue() {
        return msgQueue;
    }
}
