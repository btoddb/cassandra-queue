package com.real.cassandra.queue;

import org.scale7.cassandra.pelops.Bytes;

/**
 * Attributes that uniquely describe a 'pipe'.
 * 
 * @author Todd Burruss
 */
public class PipeDescriptor {

    private long pipeNum;
    private Bytes rowKey;
    private String rowKeyAsStr;

    public PipeDescriptor(long pipeNum, Bytes rowKey) {
        this.pipeNum = pipeNum;
        this.rowKey = rowKey;
        this.rowKeyAsStr = new String(rowKey.getBytes());
    }

    public PipeDescriptor(long pipeNum, String rowKey) {
        this.pipeNum = pipeNum;
        this.rowKey = Bytes.fromUTF8(rowKey);
        this.rowKeyAsStr = rowKey;
    }

    public long getPipeNum() {
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
        result = prime * result + (int) (pipeNum ^ (pipeNum >>> 32));
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
        PipeDescriptor other = (PipeDescriptor) obj;
        if (pipeNum != other.pipeNum)
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
        builder.append("PipeDescriptor [pipeNum=");
        builder.append(pipeNum);
        builder.append(", rowKeyAsStr=");
        builder.append(rowKeyAsStr);
        builder.append("]");
        return builder.toString();
    }

}
