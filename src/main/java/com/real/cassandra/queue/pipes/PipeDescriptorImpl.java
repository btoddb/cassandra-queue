package com.real.cassandra.queue.pipes;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.real.cassandra.queue.Descriptor;

public class PipeDescriptorImpl implements Descriptor {
    private String qName;
    private UUID pipeId;
    private AtomicInteger pushCount = new AtomicInteger(0);
    private int popCount;
    private PipeStatus pushStatus;
    private PipeStatus popStatus;
    private UUID popOwner;
    private long pushStartTimestamp;
    private Long popOwnTimestamp;

    public PipeDescriptorImpl(String qName, UUID pipeId, PipeStatus pushStatus, PipeStatus popStatus) {
        this(qName, pipeId);
        this.pushStatus = pushStatus;
        this.popStatus = popStatus;
    }

    public PipeDescriptorImpl(String qName, UUID pipeId) {
        this.qName = qName;
        this.pipeId = pipeId;
        this.popCount = 0;
        this.pushStatus = PipeStatus.ACTIVE;
        this.popStatus = PipeStatus.ACTIVE;
        this.pushStartTimestamp = System.currentTimeMillis();
    }

    @Override
    public UUID getId() {
        return getPipeId();
    }

    public UUID getPipeId() {
        return pipeId;
    }

    public String getQName() {
        return qName;
    }

    public int incPushCount() {
        return pushCount.incrementAndGet();
    }

    public int incPopCount() {
        return ++popCount;
    }

    public int getPushCount() {
        return pushCount.get();
    }

    public long getPushStartTimestamp() {
        return pushStartTimestamp;
    }

    public void setPushStartTimestamp(long pushStartTimestamp) {
        this.pushStartTimestamp = pushStartTimestamp;
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

    public void setPushCount(int msgCount) {
        this.pushCount.set(msgCount);
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

    public int getPopCount() {
        return popCount;
    }

    public void setPopCount(int popCount) {
        this.popCount = popCount;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PipeDescriptorImpl [qName=");
        builder.append(qName);
        builder.append(", pipeId=");
        builder.append(pipeId);
        builder.append(", pushCount=");
        builder.append(pushCount);
        builder.append(", pushStatus=");
        builder.append(pushStatus);
        builder.append(", popCount=");
        builder.append(popCount);
        builder.append(", popStatus=");
        builder.append(popStatus);
        builder.append(", startTimestamp=");
        builder.append(pushStartTimestamp);
        builder.append("]");
        return builder.toString();
    }

    public UUID getPopOwner() {
        return popOwner;
    }

    public void setPopOwner(UUID popOwner) {
        this.popOwner = popOwner;
    }

    public void setPopOwnTimestamp(long popOwnTimestamp) {
        this.popOwnTimestamp = popOwnTimestamp;
    }

    public Long getPopOwnTimestamp() {
        return popOwnTimestamp;
    }
}
