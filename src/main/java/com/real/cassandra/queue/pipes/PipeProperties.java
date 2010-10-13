package com.real.cassandra.queue.pipes;

public class PipeProperties {
    private PipeStatus popStatus;
    private PipeStatus pushStatus;
    private int pushCount;
    private long startTimestamp;

    public PipeProperties(PipeStatus pushStatus, PipeStatus popStatus, int pushCount, long startTimestamp) {
        this.pushStatus = pushStatus;
        this.popStatus = popStatus;
        this.pushCount = pushCount;
        this.startTimestamp = startTimestamp;
    }

    public PipeStatus getPopStatus() {
        return popStatus;
    }

    public PipeStatus getPushStatus() {
        return pushStatus;
    }

    public int getPushCount() {
        return pushCount;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PipeProperties [pushStatus=");
        builder.append(pushStatus);
        builder.append(", popStatus=");
        builder.append(popStatus);
        builder.append(", pushCount=");
        builder.append(pushCount);
        builder.append(", startTimestamp=");
        builder.append(startTimestamp);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((popStatus == null) ? 0 : popStatus.hashCode());
        result = prime * result + pushCount;
        result = prime * result + ((pushStatus == null) ? 0 : pushStatus.hashCode());
        result = prime * result + (int) (startTimestamp ^ (startTimestamp >>> 32));
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
        PipeProperties other = (PipeProperties) obj;
        if (popStatus != other.popStatus)
            return false;
        if (pushCount != other.pushCount)
            return false;
        if (pushStatus != other.pushStatus)
            return false;
        if (startTimestamp != other.startTimestamp)
            return false;
        return true;
    }

}
