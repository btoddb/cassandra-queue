package com.real.cassandra.queue;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppProperties {
    private static Logger logger = LoggerFactory.getLogger(AppProperties.class);

    private Properties rawProps;
    private String[] hostArr;

    public AppProperties(Properties rawProps) {
        this.rawProps = rawProps;
    }

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

    public int getThriftPort() {
        return getPropertyAsInt("thriftPort", 9160);
    }

    public int getReplicationFactor() {
        return getPropertyAsInt("replicationFactor", 3);
    }

    public int getNumPushers() {
        return getPropertyAsInt("numPushers", 1);
    }

    public int getNumMsgsPerPusher() {
        return getPropertyAsInt("numMsgsPerPusher", 10);
    }

    public int getNumMsgsPerPopper() {
        return getPropertyAsInt("numMsgsPerPopper", 10);
    }

    public int getNumPoppers() {
        return getPropertyAsInt("numPoppers", 1);
    }

    public long getPushDelay() {
        return getPropertyAsInt("pushDelay", 0);
    }

    public long getPopDelay() {
        return getPropertyAsInt("popDelay", 0);
    }

    public boolean getNearFifo() {
        return getPropertAsBoolean("nearFifo", true);
    }

    public boolean getDropKeyspace() {
        return getPropertAsBoolean("dropKeyspace", false);
    }

    public boolean getTruncateQueue() {
        return getPropertAsBoolean("truncateQueue", false);
    }

    public int getNumPipes() {
        return getPropertyAsInt("numPipes", 1);
    }

    public int getMinCacheConnsPerHost() {
        return getPropertyAsInt("minCacheConnsPerHost", 0);
    }

    public int getMaxConnectionsPerHost() {
        return getPropertyAsInt("maxConnsPerHost", 5);
    }

    public int getTargetConnectionsPerHost() {
        return getPropertyAsInt("targetConnsPerHost", 5);
    }

    public boolean getKillNodeConnectionsOnException() {
        return getPropertAsBoolean("killNodeConnsOnException", true);
    }

    public boolean getUseFramedTransport() {
        return getPropertAsBoolean("useFramedTransport", true);
    }

    private boolean getPropertAsBoolean(String propName, boolean defaultValue) {
        String asStr = rawProps.getProperty(propName);
        if (null == asStr) {
            return defaultValue;
        }
        else {
            return Boolean.parseBoolean(asStr);
        }
    }

    private int getPropertyAsInt(String propName, int defaultValue) {
        String asStr = rawProps.getProperty(propName);
        if (null == asStr) {
            logger.info("'" + propName + "' property not specified, using " + defaultValue);
            return defaultValue;
        }

        return Integer.parseInt(asStr);
    }
}
