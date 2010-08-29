package com.real.cassandra.queue;

import java.util.UUID;

/**
 * Model object for a message in the Cassandra queue
 * 
 * @author
 */
public class CassQMsg {

    private PipeDescriptor pipeDesc;
    private UUID msgId;
    private String value;

    public CassQMsg(PipeDescriptor pipeDesc, UUID msgId, String value) {
        this.pipeDesc = pipeDesc;
        this.msgId = msgId;
        this.value = value;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public String getValue() {
        return value;
    }

    public PipeDescriptor getQueuePipeDescriptor() {
        return pipeDesc;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CassQMsg [queuePipeDesc =");
        builder.append(pipeDesc);
        builder.append(", msgId=");
        builder.append(msgId);
        builder.append(", value=");
        builder.append(value);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((msgId == null) ? 0 : msgId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CassQMsg other = (CassQMsg) obj;
        if (msgId == null) {
            if (other.msgId != null)
                return false;
        }
        else if (!msgId.equals(other.msgId))
            return false;
        return true;
    }
}
