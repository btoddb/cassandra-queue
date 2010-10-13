package com.real.cassandra.queue.pipes;

import java.util.UUID;

public class PipeDescriptorImpl {
    private String qName;
    private UUID pipeId;
    private int msgCount;
    private PipeStatus pushStatus;
    private PipeStatus popStatus;
    private long startTimestamp;

    public PipeDescriptorImpl(String qName, UUID pipeId, PipeStatus pushStatus, PipeStatus popStatus) {
        this.qName = qName;
        this.pipeId = pipeId;
        this.msgCount = 0;
        this.pushStatus = pushStatus;
        this.popStatus = popStatus;
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

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
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

    public void setMsgCount(int msgCount) {
        this.msgCount = msgCount;
    }

    public boolean isPushActive() {
        return PipeStatus.ACTIVE.equals(pushStatus);
    }

    public boolean isPushCompleted() {
        return PipeStatus.COMPLETED.equals(pushStatus);
    }

    public boolean isPopActive() {
        return PipeStatus.ACTIVE.equals(popStatus);
    }

    public boolean isPopCompleted() {
        return PipeStatus.COMPLETED.equals(popStatus);
    }

    public PipeStatus getPushStatus() {
        return pushStatus;
    }

    public void setPushStatus(PipeStatus pushStatus) {
        this.pushStatus = pushStatus;
    }

    public PipeStatus getPopStatus() {
        return popStatus;
    }

    public void setPopStatus(PipeStatus popStatus) {
        this.popStatus = popStatus;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PipeDescriptorImpl [qName=");
        builder.append(qName);
        builder.append(", pipeId=");
        builder.append(pipeId.toString());
        builder.append(", msgCount=");
        builder.append(msgCount);
        builder.append(", pushStatus=");
        builder.append(pushStatus);
        builder.append(", popStatus=");
        builder.append(popStatus);
        builder.append(", startTimestamp=");
        builder.append(startTimestamp);
        builder.append("]");
        return builder.toString();
    }
}
