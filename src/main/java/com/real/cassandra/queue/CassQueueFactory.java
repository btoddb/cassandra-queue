package com.real.cassandra.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.repository.QueueRepository;

public class CassQueueFactory {
    private static Logger logger = LoggerFactory.getLogger(CassQueueFactory.class);

    private QueueRepository qRepos;

    public CassQueueFactory(QueueRepository qRepos) {
        this.qRepos = qRepos;
    }

    public CassQueue createInstance(String qName, EnvProperties envProps, boolean popLocks, boolean distributed)
            throws Exception {
        if (null == qRepos.getQueueDescriptor(qName)) {
            qRepos.createQueue(qName, envProps.getNumPipes());
        }

        CassQueue cq = new CassQueue(qRepos, qName, popLocks, distributed, envProps);
        if (envProps.getTruncateQueue()) {
            try {
                cq.truncate();
            }
            catch (Exception e) {
                logger.error("exception while truncating queue", e);
            }
        }

        return cq;
    }
}
