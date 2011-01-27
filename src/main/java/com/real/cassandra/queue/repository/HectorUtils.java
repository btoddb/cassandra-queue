package com.real.cassandra.queue.repository;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hom.EntityManagerImpl;

import com.real.cassandra.queue.app.QueueProperties;

public class HectorUtils {

    public static final byte[] EMPTY_BYTES = new byte[] {};

    public static QueueRepositoryImpl createQueueRepository(String hosts, int port, int replicationFactor) {
        Properties rawProps = new Properties();
        rawProps.put(QueueProperties.ENV_hosts, hosts);
        rawProps.put(QueueProperties.ENV_RPC_PORT, String.valueOf(port));
        rawProps.put(QueueProperties.ENV_REPLICATION_FACTOR, String.valueOf(replicationFactor));
        QueueProperties envProps = new QueueProperties(rawProps);
        return createQueueRepository(envProps);
    }

    public static QueueRepositoryImpl createQueueRepository(QueueProperties envProps) {
        CassandraHostConfigurator hc = new CassandraHostConfigurator(envProps.getHostArr());
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
        EntityManagerImpl entityMgr = new EntityManagerImpl(keyspace, "com.real.cassandra.queue");
        QueueRepositoryImpl qRepos = new QueueRepositoryImpl(c, envProps.getReplicationFactor(), keyspace, entityMgr);
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
