package com.real.cassandra.queue.pipeperpusher;

import java.util.UUID;

import org.apache.cassandra.thrift.Column;
import org.scale7.cassandra.pelops.Bytes;

public class PipeDescriptorFactory {

    public PipeDescriptorImpl createInstance(String qName, UUID pipeId) {
        PipeDescriptorImpl pDesc = new PipeDescriptorImpl(qName, pipeId);
        return pDesc;
    }

    public PipeDescriptorImpl createInstance(String qName, Column colActive, Column colStats) {
        UUID pipeId = Bytes.fromBytes(colActive.getName()).toUuid();
        PipeDescriptorImpl pDesc = createInstance(qName, pipeId);

        pDesc.setMsgCount(Bytes.fromBytes(colStats.getValue()).toInt());
        pDesc.setActive(QueueRepositoryImpl.ACTIVE_PIPES_TRUE_VALUE.equals(Bytes.fromBytes(colActive.getValue())
                .toUTF8()));
        return pDesc;
    }
}
