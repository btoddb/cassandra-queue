package com.real.cassandra.queue.pipeperpusher;

import java.util.UUID;

public class PipeDescriptorImpl {
    public static final String STATUS_PUSH_ACTIVE = "PA";
    public static final String STATUS_PUSH_FINISHED = "PF";
    public static final String STATUS_FINISHED_AND_EMPTY = "E";

    private String qName;
    private UUID pipeId;
    private String pipeIdAsStr;
    private int msgCount;
    private String status;

    public PipeDescriptorImpl(String qName, UUID pipeId, String status) {
        this.qName = qName;
        this.pipeId = pipeId;
        this.pipeIdAsStr = pipeId.toString();
        this.msgCount = 0;
        this.status = status;
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

    public void setMsgCount(int msgCount) {
        this.msgCount = msgCount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public boolean isPushActive() {
        return STATUS_PUSH_ACTIVE.equals(status);
    }

    public boolean isPushFinished() {
        return STATUS_PUSH_FINISHED.equals(status);
    }

    public boolean isFinishedAndEmpty() {
        return STATUS_FINISHED_AND_EMPTY.equals(status);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PipeDescriptorImpl [qName=");
        builder.append(qName);
        builder.append(", pipeIdAsStr=");
        builder.append(pipeIdAsStr);
        builder.append(", status=");
        builder.append(status);
        builder.append(", msgCount=");
        builder.append(msgCount);
        builder.append("]");
        return builder.toString();
    }
}
