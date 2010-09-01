package com.real.cassandra.queue.roundrobin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.CassQueue;
import com.real.cassandra.queue.CassQueueFactory;
import com.real.cassandra.queue.EnvProperties;
import com.real.cassandra.queue.Popper;
import com.real.cassandra.queue.Pusher;

public class CassQueueFactoryImpl implements CassQueueFactory {
    private static Logger logger = LoggerFactory.getLogger(CassQueueFactoryImpl.class);

    private QueueRepositoryImpl qRepos;

    public CassQueueFactoryImpl(QueueRepositoryImpl qRepos) {
        this.qRepos = qRepos;
    }

    @Override
    public CassQueueImpl createQueueInstance(String qName, EnvProperties envProps, boolean popLocks,
            boolean distributed, PipeManagerImpl pipeMgr) throws Exception {
        if (!qRepos.isQueueExists(qName)) {
            qRepos.createQueue(qName, envProps.getNumPipes());
        }

        CassQueueImpl cq = new CassQueueImpl(qRepos, qName, popLocks, distributed, envProps, pipeMgr);
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

    @Override
    public Popper createPopper(CassQueue q) {
        // do nothing
        return null;
    }

    @Override
    public Pusher createPusher(CassQueue q) {
        // do nothing
        return null;
    }
}
