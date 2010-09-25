package com.real.cassandra.queue.pipes;

import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;

import me.prettyprint.cassandra.model.HColumn;

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
        qRepos.createPipeDescriptor(qName, pipeId, PipeDescriptorImpl.STATUS_PUSH_ACTIVE, System.currentTimeMillis());

        return createInstance(qName, pipeId, status, msgCount, System.currentTimeMillis());
    }

    public PipeDescriptorImpl createInstance(String qName, UUID pipeId, String status, int msgCount, long startTimestamp) {
        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(qName, pipeId, status);
        pipeDesc.setMsgCount(msgCount);
        return pipeDesc;
    }

    public PipeDescriptorImpl createInstance(String qName, HColumn<UUID, String> col) {
        PipeStatus ps = pipeStatusFactory.createInstance(col);
        return createInstance(qName, col.getName(), ps.getStatus(), ps.getPushCount(), ps.getStartTimestamp());
    }

}
