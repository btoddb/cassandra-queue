package com.real.cassandra.queue.pipeperpusher;

import java.util.UUID;

public class PipeDescriptorImpl {
    private String qName;
    private UUID pipeId;
    private String pipeIdAsStr;
    private int msgCount;
    private boolean active;

    public PipeDescriptorImpl(String qName, UUID pipeId) {
        this.qName = qName;
        this.pipeId = pipeId;
        this.pipeIdAsStr = pipeId.toString();
        this.msgCount = 0;
        this.active = true;
    }

    public UUID getPipeId() {
        return pipeId;
    }

    public String getQName() {
        return qName;
    }

    public int incMsgCount() {
        return ++msgCount;
    }

    public int getMsgCount() {
        return msgCount;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((pipeId == null) ? 0 : pipeId.hashCode());
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
        PipeDescriptorImpl other = (PipeDescriptorImpl) obj;
        if (pipeId == null) {
            if (other.pipeId != null)
                return false;
        }
        else if (!pipeId.equals(other.pipeId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PipeDescriptorImpl [qName=");
        builder.append(qName);
        builder.append(", pipeIdAsStr=");
        builder.append(pipeIdAsStr);
        builder.append(", msgCount=");
        builder.append(msgCount);
        builder.append("]");
        return builder.toString();
    }

    public void setMsgCount(int msgCount) {
        this.msgCount = msgCount;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

}
