package com.real.cassandra.queue.pipes;

import java.util.UUID;

import me.prettyprint.cassandra.model.HColumn;

import org.apache.cassandra.thrift.Column;
import org.scale7.cassandra.pelops.Bytes;

import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;
import com.real.cassandra.queue.utils.UuidGenerator;

public class PipeDescriptorFactory {

    private QueueRepositoryAbstractImpl qRepos;
    private PipeStatusFactory pipeStatusFactory = new PipeStatusFactory();

    public PipeDescriptorFactory(QueueRepositoryAbstractImpl qRepos) {
        this.qRepos = qRepos;
    }

    public PipeDescriptorImpl createInstance(String qName, String status, int msgCount) throws Exception {
        UUID pipeId = UuidGenerator.generateTimeUuid();
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
        return createInstance(qName, pipeId, ps.getStatus(), ps.getPushCount());
    }

    public PipeDescriptorImpl createInstance(String qName, UUID pipeId, String status, int msgCount) {
        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(qName, pipeId, status);
        pipeDesc.setMsgCount(msgCount);
        return pipeDesc;
    }

    public PipeDescriptorImpl createInstance(String qName, HColumn<UUID, String> col) {
        PipeStatus ps = pipeStatusFactory.createInstance(col);
        return createInstance(qName, col.getName(), ps.getStatus(), ps.getPushCount());
    }
}
