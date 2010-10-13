package com.real.cassandra.queue.repository;

import java.util.Arrays;
import java.util.Collection;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import com.real.cassandra.queue.app.EnvProperties;

public class HectorUtils {

    public static QueueRepositoryImpl createQueueRepository(EnvProperties envProps) throws Exception {
        CassandraHostConfigurator hc = new CassandraHostConfigurator(outputStringsAsCommaDelim(envProps.getHostArr()));
        hc.setCassandraThriftSocketTimeout(envProps.getCassandraThriftSocketTimeout());
        hc.setExhaustedPolicy(envProps.getExhaustedPolicy());
        hc.setLifo(envProps.getLifo());
        hc.setMaxActive(envProps.getMaxActive());
        hc.setMaxIdle(envProps.getMaxIdle());
        hc.setMaxWaitTimeWhenExhausted(envProps.getMaxWaitTimeWhenExhausted());
        hc.setMinEvictableIdleTimeMillis(envProps.getMinEvictableIdleTimeMillis());
        hc.setPort(envProps.getRpcPort());
        hc.setRetryDownedHosts(envProps.getRetryDownedHosts());
        hc.setRetryDownedHostsDelayInSeconds(envProps.getRetryDownedHostsDelayInSeconds());
        hc.setRetryDownedHostsQueueSize(envProps.getRetryDownedHostsQueueSize());
        hc.setTimeBetweenEvictionRunsMillis(envProps.getTimeBetweenEvictionRunsMillis());
        hc.setUseThriftFramedTransport(envProps.getUseThriftFramedTransport());

        Cluster c = HFactory.getOrCreateCluster(QueueRepositoryImpl.QUEUE_POOL_NAME, hc);

        Keyspace keyspace = HFactory.createKeyspace(QueueRepositoryImpl.QUEUE_KEYSPACE_NAME, c);
        QueueRepositoryImpl qRepos = new QueueRepositoryImpl(c, envProps.getReplicationFactor(), keyspace);
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
                sb.append(",");
            }
            else {
                sb = new StringBuilder();
            }

            sb.append(str);
        }

        return sb.toString();
    }

}
