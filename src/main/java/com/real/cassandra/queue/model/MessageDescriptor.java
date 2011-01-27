package com.real.cassandra.queue.model;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.real.cassandra.queue.repository.QueueRepositoryImpl;

@Entity
@Table(name = QueueRepositoryImpl.MSG_DESCRIPTOR_COLFAM)
public class MessageDescriptor {

    @Id
    private UUID msgId;
    
    @Column(name = QueueRepositoryImpl.MDESC_COLNAME_POP_TIMESTAMP)
    private Long popTimestamp;

    @Column(name = "commitTimestamp")
    private Long commitTimestamp;
    
    @Column(name = "createTimestamp")
    private Long createTimestamp;
    
    @Column(name = "payload")
    private byte[] payload;

    
    public UUID getMsgId() {
        return msgId;
    }

    public void setMsgId(UUID msgId) {
        this.msgId = msgId;
    }

    public Long getPopTimestamp() {
        return popTimestamp;
    }

    public void setPopTimestamp(long popTimestamp) {
        this.popTimestamp = popTimestamp;
    }

    public Long getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public byte[] getPayload() {
        return payload;
    }
    
    public ByteBuffer getPayloadAsByteBuffer() {
        return ByteBuffer.wrap(payload);
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    public Long getCreateTimestamp() {
        return createTimestamp;
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
        MessageDescriptor other = (MessageDescriptor) obj;
        if (msgId == null) {
            if (other.msgId != null)
                return false;
        }
        else if (!msgId.equals(other.msgId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MessageDescriptor [commitTimestamp=" + commitTimestamp + ", createTimestamp=" + createTimestamp
                + ", msgId=" + msgId + ", payload=" + Arrays.toString(payload) + ", popTimestamp=" + popTimestamp + "]";
    }

    public void setPopTimestamp(Long popTimestamp) {
        this.popTimestamp = popTimestamp;
    }

    public void setCommitTimestamp(Long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public void setCreateTimestamp(Long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }
}
