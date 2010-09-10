package com.real.cassandra.queue.repository.hector;

import java.util.Arrays;
import java.util.Collection;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.real.cassandra.queue.app.EnvProperties;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;

public class HectorUtils {

    public static QueueRepositoryImpl createQueueRepository(EnvProperties envProps, ConsistencyLevel consistencyLevel)
            throws Exception {
        CassandraHostConfigurator hc = new CassandraHostConfigurator(outputStringsAsCommaDelim(envProps.getHostArr()));
        hc.setPort(envProps.getRpcPort());
        Cluster c = HFactory.getOrCreateCluster(QueueRepositoryAbstractImpl.QUEUE_POOL_NAME, hc);

        QueueRepositoryImpl qRepos = new QueueRepositoryImpl(c, envProps.getReplicationFactor(), consistencyLevel);
        qRepos.initKeyspace(envProps.getDropKeyspace());
        return qRepos;
    }

    public static String outputStringsAsCommaDelim(String[] strArr) {
        return outputStringsAsCommaDelim(Arrays.asList(strArr));
    }

    public static String outputStringsAsCommaDelim(Collection<String> collection) {
        if (null == collection) {
            return null;
        }

        if (collection.isEmpty()) {
            return "";
        }

        StringBuilder sb = null;
        for (String str : collection) {
            if (null != sb) {
                sb.append(", ");
            }
            else {
                sb = new StringBuilder();
            }

            sb.append(str);
        }

        return sb.toString();
    }

}
