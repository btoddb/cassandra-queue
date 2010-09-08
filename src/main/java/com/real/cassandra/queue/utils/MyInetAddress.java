package com.real.cassandra.queue.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyInetAddress {
    private static Logger logger = LoggerFactory.getLogger(MyInetAddress.class);

    private InetAddress inetAddr;

    public MyInetAddress() {
        try {
            inetAddr = InetAddress.getLocalHost();
        }
        catch (UnknownHostException e) {
            logger.error("exception while getting local IP address", e);
            throw new RuntimeException(e);
        }
    }

    public InetAddress get() {
        return inetAddr;
    }
}
