package com.real.cassandra.queue;

import com.real.cassandra.queue.repository.QueueRepository;

public class CassQueueFactory {

    private QueueRepository qRepos;

    public CassQueueFactory(QueueRepository qRepos) {
        this.qRepos = qRepos;
    }

    public CassQueue createInstance(String qName, EnvProperties envProps, boolean popLocks, boolean distributed)
            throws Exception {
        qRepos.createQueue(qName, envProps.getNumPipes());
        return new CassQueue(qRepos, qName, popLocks, distributed, envProps);
    }
}
