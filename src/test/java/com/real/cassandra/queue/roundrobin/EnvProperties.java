package com.real.cassandra.queue.roundrobin;

import java.util.Map.Entry;
import java.util.Properties;

import javax.management.InstanceAlreadyExistsException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.cassandra.queue.JmxMBeanManager;

public class EnvProperties implements EnvPropertiesMXBean {
    private static Logger logger = LoggerFactory.getLogger(EnvProperties.class);

    public static final String ENV_hosts = "hosts";
    public static final String ENV_thriftPort = "thriftPort";
    public static final String ENV_replicationFactor = "replicationFactor";

    public static final String ENV_nearFifo = "nearFifo";
    public static final String ENV_numPipes = "numPipes";

    public static final String ENV_pushPipeIncrementDelay = "pushPipeIncrementDelay";

    public static final String ENV_numPushers = "numPushers";
    public static final String ENV_numPoppers = "numPoppers";
    public static final String ENV_numMsgsPerPusher = "numMsgsPerPusher";
    public static final String ENV_numMsgsPerPopper = "numMsgsPerPopper";
    public static final String ENV_pushDelay = "pushDelay";
    public static final String ENV_popDelay = "popDelay";

    public static final String ENV_minCacheConnsPerHost = "minCacheConnsPerHost";
    public static final String ENV_targetConnsPerHost = "targetConnsPerHost";
    public static final String ENV_maxConnsPerHost = "maxConnsPerHost";
    public static final String ENV_killNodeConnsOnException = "killNodeConnsOnException";
    public static final String ENV_useFramedTransport = "useFramedTransport";

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
    public int getThriftPort() {
        return getPropertyAsInt("thriftPort", 9160);
    }

    @Override
    public int getReplicationFactor() {
        return getPropertyAsInt("replicationFactor", 3);
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
    public int getNumMsgsPerPusher() {
        return getPropertyAsInt("numMsgsPerPusher", 10);
    }

    @Override
    public void setNumMsgsPerPusher(int value) {
        setIntProperty(ENV_numMsgsPerPusher, value);
    }

    @Override
    public int getNumMsgsPerPopper() {
        return getPropertyAsInt("numMsgsPerPopper", 10);
    }

    @Override
    public void setNumMsgsPerPopper(int value) {
        setIntProperty(ENV_numMsgsPerPopper, value);
    }

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
    public boolean getNearFifo() {
        return getPropertAsBoolean("nearFifo", true);
    }

    @Override
    public boolean getDropKeyspace() {
        return getPropertAsBoolean("dropKeyspace", false);
    }

    @Override
    public boolean getTruncateQueue() {
        return getPropertAsBoolean("truncateQueue", false);
    }

    @Override
    public int getNumPipes() {
        return getPropertyAsInt("numPipes", 1);
    }

    @Override
    public int getMinCacheConnsPerHost() {
        return getPropertyAsInt("minCacheConnsPerHost", 0);
    }

    @Override
    public int getMaxConnectionsPerHost() {
        return getPropertyAsInt("maxConnsPerHost", 5);
    }

    @Override
    public int getTargetConnectionsPerHost() {
        return getPropertyAsInt("targetConnsPerHost", 5);
    }

    @Override
    public boolean getKillNodeConnectionsOnException() {
        return getPropertAsBoolean("killNodeConnsOnException", true);
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

    @Override
    public void setPushPipeIncrementDelay(long value) {
        setLongProperty(ENV_pushPipeIncrementDelay, value);
    }

    public boolean getPropertAsBoolean(String propName, boolean defaultValue) {
        String asStr = rawProps.getProperty(propName);
        if (null == asStr) {
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
}
