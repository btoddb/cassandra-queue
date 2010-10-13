package com.real.cassandra.queue.app;

import java.util.Map.Entry;
import java.util.Properties;

import javax.management.InstanceAlreadyExistsException;

import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.ExhaustedPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.utils.JmxMBeanManager;

public class EnvProperties implements EnvPropertiesMXBean {
    private static Logger logger = LoggerFactory.getLogger(EnvProperties.class);

    public static final String ENV_API_NAME = "apiName";
    public static final String ENV_hosts = "hosts";
    public static final String ENV_RPC_PORT = "rpcPort";
    public static final String ENV_REPLICATION_FACTOR = "replicationFactor";

    // public static final String ENV_strictFifo = "nearFifo";
    public static final String ENV_QUEUE_NAME = "qName";
    public static final String ENV_numPipes = "numPipes";

    public static final String ENV_numPushers = "numPushers";
    public static final String ENV_numPoppers = "numPoppers";
    public static final String ENV_numMsgs = "numMsgs";
    public static final String ENV_pushDelay = "pushDelay";
    public static final String ENV_popDelay = "popDelay";
    public static final String ENV_maxPopWidth = "maxPopWidth";
    public static final String ENV_maxPushesPerPipe = "maxPushesPerPipe";
    public static final String ENV_maxPushTimePerPipe = "maxPushTimePerPipe";
    public static final String ENV_popPipeRefreshDelay = "popPipeRefreshDelay";

    public static final String ENV_cassandraThriftSocketTimeout = "cassandraThriftSocketTimeout";
    public static final String ENV_exhaustedPolicy = "exhaustedPolicy";
    public static final String ENV_lifo = "lifo";
    public static final String ENV_maxActive = "maxActive";
    public static final String ENV_maxIdle = "maxIdle";
    public static final String ENV_maxWaitTimeWhenExhausted = "maxWaitTimeWhenExhausted";
    public static final String ENV_minEvictableIdleTimeMillis = "minEvictableIdleTimeMillis";
    public static final String ENV_retryDownedHosts = "retryDownedHosts";
    public static final String ENV_retryDownedHostsDelayInSeconds = "retryDownedHostsDelayInSeconds";
    public static final String ENV_retryDownedHostsQueueSize = "retryDownedHostsQueueSize";
    public static final String ENV_timeBetweenEvictionRunsMillis = "timeBetweenEvictionRunsMillis";
    public static final String ENV_useThriftFramedTransport = "useThriftFramedTransport";

    public static final String ENV_dropKeyspace = "dropKeyspace";
    public static final String ENV_truncateQueue = "truncateQueue";

    private Properties rawProps;
    private String[] hostArr;

    /**
     * Load from raw properties file.
     * 
     * @param rawProps
     */
    public EnvProperties(Properties rawProps) {
        this.rawProps = cloneProperties(rawProps);
        initJmx();
    }

    @Override
    public EnvProperties clone() {
        return new EnvProperties(cloneProperties(this.rawProps));
    }

    public static Properties cloneProperties(Properties rawProps) {
        Properties newProps = new Properties();
        for (Entry<Object, Object> entry : rawProps.entrySet()) {
            newProps.put(entry.getKey(), entry.getValue());
        }
        return newProps;
    }

    private void initJmx() {
        String beanName = JMX_MBEAN_OBJ_NAME;
        try {
            JmxMBeanManager.getInstance().registerMBean(this, beanName);
        }
        catch (InstanceAlreadyExistsException e1) {
            logger.warn("exception while registering MBean, " + beanName + " - ignoring");
        }
        catch (Exception e) {
            throw new RuntimeException("exception while registering MBean, " + beanName);
        }
    }

    @Override
    public String[] getHostArr() {
        if (null == hostArr) {
            String hosts = rawProps.getProperty("hosts");
            if (null != hosts) {
                hostArr = hosts.trim().split("\\s*,\\s*");
            }
            else {
                logger.info("'hosts' property not specified, using localhost");
                hostArr = new String[] {
                    "localhost" };
            }

        }
        return hostArr;
    }

    @Override
    public int getRpcPort() {
        return getPropertyAsInt(ENV_RPC_PORT, 9160);
    }

    @Override
    public int getReplicationFactor() {
        return getPropertyAsInt(ENV_REPLICATION_FACTOR, 3);
    }

    public String getQName() {
        return rawProps.getProperty(ENV_QUEUE_NAME);
    }

    @Override
    public int getNumPushers() {
        return getPropertyAsInt("numPushers", 1);
    }

    @Override
    public void setNumPushers(int value) {
        setIntProperty(ENV_numPushers, value);
    }

    @Override
    public int getNumMsgs() {
        return getPropertyAsInt(ENV_numMsgs, 10);
    }

    public void setNumMsgs(int numMsgs) {
        setIntProperty(ENV_numMsgs, numMsgs);
    }

    // @Override
    // public void setNumMsgsPerPopper(int value) {
    // setIntProperty(ENV_numMsgsPerPopper, value);
    // }

    @Override
    public int getNumPoppers() {
        return getPropertyAsInt("numPoppers", 1);
    }

    @Override
    public void setNumPoppers(int value) {
        setIntProperty(ENV_numPoppers, value);
    }

    @Override
    public long getPushDelay() {
        return getPropertyAsInt("pushDelay", 0);
    }

    @Override
    public void setPushDelay(long value) {
        setLongProperty(ENV_pushDelay, value);
    }

    @Override
    public long getPopDelay() {
        return getPropertyAsInt("popDelay", 0);
    }

    @Override
    public void setPopDelay(long value) {
        setLongProperty(ENV_popDelay, value);
    }

    @Override
    public boolean getDropKeyspace() {
        return getPropertAsBoolean("dropKeyspace", false);
    }

    @Override
    public boolean getTruncateQueue() {
        return getPropertAsBoolean("truncateQueue", false);
    }

    // @Override
    // public int getMinCacheConnsPerHost() {
    // return getPropertyAsInt("minCacheConnsPerHost", 0);
    // }

    @Override
    public int getMaxConnectionsPerHost() {
        return getPropertyAsInt("maxConnsPerHost", 5);
    }

    @Override
    public int getTargetConnectionsPerHost() {
        return getPropertyAsInt("targetConnsPerHost", 5);
    }

    @Override
    public boolean getUseFramedTransport() {
        return getPropertAsBoolean("useFramedTransport", true);
    }

    @Override
    public long getPipeCheckDelay() {
        return getPropertyAsLong("pipeCheckDelay", 100);
    }

    @Override
    public long getPushPipeIncrementDelay() {
        return getPropertyAsLong("pushPipeIncrementDelay", 1 * 1000 * 60);
    }

    // @Override
    // public void setPushPipeIncrementDelay(long value) {
    // setLongProperty(ENV_pushPipeIncrementDelay, value);
    // }

    public int getMaxPushesPerPipe() {
        return getPropertyAsInt(ENV_maxPushesPerPipe, 100);
    }

    public int getMaxPopWidth() {
        return getPropertyAsInt(ENV_maxPopWidth, 4);
    }

    public long getPopPipeRefreshDelay() {
        return getPropertyAsLong(ENV_popPipeRefreshDelay, 5000);
    }

    public long getMaxPushTimePerPipe() {
        return getPropertyAsLong(ENV_maxPushTimePerPipe, 10 * 60000);
    }

    public String getApiName() {
        return rawProps.getProperty(ENV_API_NAME);
    }

    public boolean getPropertAsBoolean(String propName, boolean defaultValue) {
        String asStr = rawProps.getProperty(propName);
        if (null == asStr) {
            logger.info("'" + propName + "' property not specified, using " + defaultValue);
            return defaultValue;
        }
        else {
            return Boolean.parseBoolean(asStr);
        }
    }

    public int getPropertyAsInt(String propName, int defaultValue) {
        String asStr = rawProps.getProperty(propName);
        if (null == asStr) {
            logger.info("'" + propName + "' property not specified, using " + defaultValue);
            return defaultValue;
        }

        return Integer.parseInt(asStr);
    }

    public long getPropertyAsLong(String propName, long defaultValue) {
        String asStr = rawProps.getProperty(propName);
        if (null == asStr) {
            logger.info("'" + propName + "' property not specified, using " + defaultValue);
            return defaultValue;
        }

        return Long.parseLong(asStr);
    }

    private void setStrProperty(String key, String value) {
        rawProps.setProperty(key, value);
    }

    private void setIntProperty(String key, int value) {
        setStrProperty(key, String.valueOf(value));
    }

    private void setLongProperty(String key, long value) {
        setStrProperty(key, String.valueOf(value));
    }

    public int getCassandraThriftSocketTimeout() {
        return getPropertyAsInt(ENV_cassandraThriftSocketTimeout, 0);
    }

    public boolean getLifo() {
        return getPropertAsBoolean(ENV_lifo, CassandraHost.DEFAULT_LIFO);
    }

    public ExhaustedPolicy getExhaustedPolicy() {
        String value = rawProps.getProperty(ENV_exhaustedPolicy, "WHEN_EXHAUSTED_FAIL");
        return ExhaustedPolicy.valueOf(value);
    }

    public int getMaxActive() {
        return getPropertyAsInt(ENV_maxActive, CassandraHost.DEFAULT_MAX_ACTIVE);
    }

    public int getMaxIdle() {
        return getPropertyAsInt(ENV_maxIdle, CassandraHost.DEFAULT_MAX_IDLE);
    }

    public long getMaxWaitTimeWhenExhausted() {
        return getPropertyAsLong(ENV_maxWaitTimeWhenExhausted, CassandraHost.DEFAULT_MAX_WAITTIME_WHEN_EXHAUSTED);
    }

    public long getMinEvictableIdleTimeMillis() {
        return getPropertyAsLong(ENV_minEvictableIdleTimeMillis, CassandraHost.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
    }

    public boolean getRetryDownedHosts() {
        return getPropertAsBoolean(ENV_retryDownedHosts, false);
    }

    public int getRetryDownedHostsDelayInSeconds() {
        return getPropertyAsInt(ENV_retryDownedHostsDelayInSeconds, 1);
    }

    public int getRetryDownedHostsQueueSize() {
        return getPropertyAsInt(ENV_retryDownedHostsQueueSize, 10);
    }

    public long getTimeBetweenEvictionRunsMillis() {
        return getPropertyAsLong(ENV_timeBetweenEvictionRunsMillis,
                CassandraHost.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
    }

    public boolean getUseThriftFramedTransport() {
        return getPropertAsBoolean(ENV_useThriftFramedTransport, true);
    }
}
