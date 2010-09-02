package com.real.cassandra.queue.pipeperpusher;

import java.util.UUID;

import org.apache.cassandra.thrift.Column;
import org.scale7.cassandra.pelops.Bytes;

public class PipeDescriptorFactory {
    private PipeStatusFactory pipeStatusFactory = new PipeStatusFactory();

    public PipeDescriptorImpl createInstance(String qName, UUID pipeId, String status, int msgCount) {
        PipeDescriptorImpl pDesc = new PipeDescriptorImpl(qName, pipeId, status);
        pDesc.setMsgCount(msgCount);
        return pDesc;
    }

    public PipeDescriptorImpl createInstance(String qName, Column colStatus) {
        if (null == colStatus) {
            throw new IllegalArgumentException("the pipe status column cannot be null");
        }
        UUID pipeId = Bytes.fromBytes(colStatus.getName()).toUuid();
        PipeStatus ps = pipeStatusFactory.createInstance(colStatus);
        return createInstance(qName, pipeId, ps.getStatus(), ps.getPushCount());
    }
}
