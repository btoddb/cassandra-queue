package com.real.cassandra.queue.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.app.EnvProperties;
import com.real.cassandra.queue.repository.hector.HectorUtils;
import com.real.cassandra.queue.repository.hector.QueueRepositoryImpl;

public class RepositoryFactoryImpl {
    private static Logger logger = LoggerFactory.getLogger(RepositoryFactoryImpl.class);

    public static final String API_HECTOR = "hector";
    public static final String API_PELOPS = "pelops";

    public QueueRepositoryImpl createInstance(EnvProperties envProps) throws Exception {
        logger.info("client API chosen : " + API_HECTOR);
        return HectorUtils.createQueueRepository(envProps);
    }
}
