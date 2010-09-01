package com.real.cassandra.queue.pipeperpusher;

import java.util.UUID;

import org.apache.cassandra.thrift.Column;
import org.scale7.cassandra.pelops.Bytes;

public class PipeDescriptorFactory {

    public PipeDescriptorImpl createInstance(String qName, UUID pipeId, String status) {
        PipeDescriptorImpl pDesc = new PipeDescriptorImpl(qName, pipeId, status);
        return pDesc;
    }

    public PipeDescriptorImpl createInstance(String qName, Column colStatus, Column colStats) {
        if (null == colStatus) {
            throw new IllegalArgumentException("the pipe status column cannot be null");
        }

        UUID pipeId = Bytes.fromBytes(colStatus.getName()).toUuid();
        PipeDescriptorImpl pDesc = createInstance(qName, pipeId, new String(colStatus.getValue()));

        if (null != colStats) {
            pDesc.setMsgCount(Bytes.fromBytes(colStats.getValue()).toInt());
        }
        return pDesc;
    }
}
