package com.real.cassandra.queue;

import org.springframework.integration.Message;
import org.springframework.integration.MessageHeaders;

public class EventAsSiMsg implements Message<String> {
    private Event evt;

    public EventAsSiMsg(Event evt) {
        this.evt = evt;
    }

    @Override
    public MessageHeaders getHeaders() {
        return null;
    }

    @Override
    public String getPayload() {
        return evt.getValue();
    }

}
