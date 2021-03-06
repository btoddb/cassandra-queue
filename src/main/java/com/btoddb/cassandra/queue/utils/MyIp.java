package com.btoddb.cassandra.queue.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyIp {
    private static Logger logger = LoggerFactory.getLogger(MyIp.class);

    private static InetAddress inetAddr;

    static {
        try {
            inetAddr = InetAddress.getLocalHost();
        }
        catch (UnknownHostException e) {
            logger.error("exception while getting local IP address", e);
            throw new RuntimeException(e);
        }
    }

    public static InetAddress get() {
        return inetAddr;
    }
}
