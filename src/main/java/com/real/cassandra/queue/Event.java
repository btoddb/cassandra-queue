package com.real.cassandra.queue;

import java.util.UUID;

public class Event {

    private String key;
    private UUID msgId;
    private String value;

    public Event(String key, UUID msgId, String value) {
        this.key = key;
        this.msgId = msgId;
        this.value = value;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public String getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }
}
