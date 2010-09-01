package com.real.cassandra.queue.roundrobin;

import org.scale7.cassandra.pelops.Bytes;

import com.real.cassandra.queue.PipeDescriptor;

/**
 * Attributes that uniquely describe a 'pipe'.
 * 
 * @author Todd Burruss
 */
public class PipeDescriptorImpl implements PipeDescriptor {

    private String pipeNum;
    private Bytes rowKey;
    private String rowKeyAsStr;

    public PipeDescriptorImpl(long pipeNum, String rowKey) {
        this(pipeNum, Bytes.fromUTF8(rowKey));
    }

    public PipeDescriptorImpl(long pipeNum, Bytes rowKey) {
        this(String.valueOf(pipeNum), rowKey);
    }

    public PipeDescriptorImpl(String pipeNum, String rowKey) {
        this(pipeNum, Bytes.fromUTF8(rowKey));
    }

    public PipeDescriptorImpl(String pipeNum, Bytes rowKey) {
        this.pipeNum = pipeNum;
        this.rowKey = rowKey;
        this.rowKeyAsStr = new String(rowKey.getBytes());
    }

    @Override
    public String getPipeId() {
        return pipeNum;
    }

    public Bytes getRowKey() {
        return rowKey;
    }

    public String getRowKeyAsStr() {
        return rowKeyAsStr;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((pipeNum == null) ? 0 : pipeNum.hashCode());
        result = prime * result + ((rowKey == null) ? 0 : rowKey.hashCode());
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
        if (pipeNum == null) {
            if (other.pipeNum != null)
                return false;
        }
        else if (!pipeNum.equals(other.pipeNum))
            return false;
        if (rowKey == null) {
            if (other.rowKey != null)
                return false;
        }
        else if (!rowKey.equals(other.rowKey))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PipeDescriptorImpl [pipeNum=");
        builder.append(pipeNum);
        builder.append(", rowKeyAsStr=");
        builder.append(rowKeyAsStr);
        builder.append("]");
        return builder.toString();
    }

}
