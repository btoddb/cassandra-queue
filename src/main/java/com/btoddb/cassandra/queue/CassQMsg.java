package com.btoddb.cassandra.queue;

import java.util.UUID;

import com.btoddb.cassandra.queue.model.MessageDescriptor;
import com.btoddb.cassandra.queue.pipes.PipeDescriptorImpl;


/**
 * Model object for a message in the Cassandra queue
 * 
 * @author
 */
public class CassQMsg {

    private PipeDescriptorImpl pipeDesc;
    private UUID msgId;
    private MessageDescriptor msgDesc;

    public CassQMsg(PipeDescriptorImpl pipeDesc, UUID msgId, MessageDescriptor msgDesc) {
        this.pipeDesc = pipeDesc;
        this.msgId = msgId;
        this.msgDesc = msgDesc;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public MessageDescriptor getMsgDesc() {
        return msgDesc;
    }

    public PipeDescriptorImpl getPipeDescriptor() {
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
        builder.append(msgDesc);
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
