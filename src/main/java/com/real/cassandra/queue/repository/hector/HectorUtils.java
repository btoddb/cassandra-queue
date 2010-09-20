package com.real.cassandra.queue.repository.hector;

import java.util.Arrays;
import java.util.Collection;

import me.prettyprint.cassandra.model.KeyspaceOperator;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

import com.real.cassandra.queue.app.EnvProperties;
import com.real.cassandra.queue.repository.QueueRepositoryAbstractImpl;

public class HectorUtils {

    public static QueueRepositoryImpl createQueueRepository(EnvProperties envProps) throws Exception {
        CassandraHostConfigurator hc = new CassandraHostConfigurator(outputStringsAsCommaDelim(envProps.getHostArr()));
        hc.setPort(envProps.getRpcPort());
        Cluster c = HFactory.getOrCreateCluster(QueueRepositoryAbstractImpl.QUEUE_POOL_NAME, hc);

        KeyspaceOperator ko = HFactory.createKeyspaceOperator(QueueRepositoryAbstractImpl.QUEUE_KEYSPACE_NAME, c);
        QueueRepositoryImpl qRepos = new QueueRepositoryImpl(c, envProps.getReplicationFactor(), ko);
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
