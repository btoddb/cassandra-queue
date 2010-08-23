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

    public int getHostPort() {
        String portStr = rawProps.getProperty("thriftPort");
        if (null == portStr) {
            logger.info("'hostPort' property not specified, using 9160");
            portStr = "9160";
        }

        return Integer.parseInt(portStr);
    }

}
