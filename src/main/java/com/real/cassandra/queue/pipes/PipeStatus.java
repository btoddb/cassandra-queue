package com.real.cassandra.queue.pipes;

public class PipeStatus {
    private String status;
    private int pushCount;

    public PipeStatus(String status, int pushCount) {
        this.status = status;
        this.pushCount = pushCount;
    }

    public String getStatus() {
        return status;
    }

    public int getPushCount() {
        return pushCount;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + pushCount;
        result = prime * result + ((status == null) ? 0 : status.hashCode());
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
        PipeStatus other = (PipeStatus) obj;
        if (pushCount != other.pushCount)
            return false;
        if (status == null) {
            if (other.status != null)
                return false;
        }
        else if (!status.equals(other.status))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PipeStatus [status=");
        builder.append(status);
        builder.append(", pushCount=");
        builder.append(pushCount);
        builder.append("]");
        return builder.toString();
    }

}
