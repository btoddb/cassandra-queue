package com.real.cassandra.queue.pipeperpusher;

import java.util.UUID;

import org.apache.cassandra.thrift.Column;
import org.scale7.cassandra.pelops.Bytes;

import com.real.cassandra.queue.pipeperpusher.utils.CassQueueUtils;

public class PipeDescriptorFactory {

    private QueueRepositoryImpl qRepos;
    private PipeStatusFactory pipeStatusFactory = new PipeStatusFactory();

    public PipeDescriptorFactory(QueueRepositoryImpl qRepos) {
        this.qRepos = qRepos;
    }

    public PipeDescriptorImpl createInstance(String qName, String status, int msgCount) throws Exception {
        UUID pipeId = CassQueueUtils.generateTimeUuid();
        qRepos.createPipeDescriptor(qName, pipeId, PipeDescriptorImpl.STATUS_PUSH_ACTIVE);

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
        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(qName, pipeId, ps.getStatus());
        pipeDesc.setMsgCount(ps.getPushCount());
        return pipeDesc;
    }

    public PipeDescriptorImpl createInstance(String qName, UUID pipeId, String status, int msgCount) {
        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(qName, pipeId, status);
        pipeDesc.setMsgCount(msgCount);
        return pipeDesc;
    }
}
