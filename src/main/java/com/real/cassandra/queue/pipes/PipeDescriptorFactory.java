package com.real.cassandra.queue.pipes;

import java.util.UUID;

import me.prettyprint.hector.api.beans.HColumn;

import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.utils.UuidGenerator;

public class PipeDescriptorFactory {

    private QueueRepositoryImpl qRepos;
    private PipePropertiesFactory pipeStatusFactory = new PipePropertiesFactory();

    public PipeDescriptorFactory(QueueRepositoryImpl qRepos) {
        this.qRepos = qRepos;
    }

    public PipeDescriptorImpl createInstance(String qName, PipeStatus pushStatus, PipeStatus popStatus, int msgCount) {
        long now = System.currentTimeMillis();
        UUID pipeId = UuidGenerator.generateTimeUuid();
        qRepos.createPipeDescriptor(qName, pipeId, pushStatus, popStatus, now);

        return createInstance(qName, pipeId, pushStatus, popStatus, msgCount, now);
    }

    public PipeDescriptorImpl createInstance(String qName, UUID pipeId, PipeStatus pushStatus, PipeStatus popStatus,
            int msgCount, long startTimestamp) {
        PipeDescriptorImpl pipeDesc = new PipeDescriptorImpl(qName, pipeId, pushStatus, popStatus);
        pipeDesc.setMsgCount(msgCount);
        pipeDesc.setStartTimestamp(startTimestamp);
        return pipeDesc;
    }

    public PipeDescriptorImpl createInstance(String qName, HColumn<UUID, String> col) {
        PipeProperties ps = pipeStatusFactory.createInstance(col);
        return createInstance(qName, col.getName(), ps.getPushStatus(), ps.getPopStatus(), ps.getPushCount(),
                ps.getStartTimestamp());
    }

}
