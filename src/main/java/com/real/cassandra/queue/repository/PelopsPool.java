package com.real.cassandra.queue.repository;

import java.security.InvalidParameterException;
import java.util.List;

import org.wyki.cassandra.pelops.GeneralPolicy;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.ThriftPool;
import org.wyki.cassandra.pelops.ThriftPoolComplex.Policy;

public class PelopsPool {
    private Policy policy = new Policy();
    private GeneralPolicy generalPolicy = new GeneralPolicy();
    private String poolName = "defaultPool";
    private List<String> hostNameList;
    private String keyspaceName;
    private boolean nodeDiscovery = false;
    private int port = 9160;
    private int socketReadTimeout = 5000;
    private boolean thriftFramedTransport = true;

    public void initPool() {
        if (null == hostNameList || hostNameList.isEmpty()) {
            throw new InvalidParameterException("hostNameList property has not been set or is empty");
        }

        if (null == policy) {
            throw new InvalidParameterException("policy property has not been set");
        }

        Pelops.addPool(poolName, hostNameList.toArray(new String[] {}), port, socketReadTimeout, nodeDiscovery,
                keyspaceName, generalPolicy, policy);
    }

    public ThriftPool getConnectionPool() {
        return Pelops.getDbConnPool(poolName);
    }

    public ThriftPool.Connection getConnection() {
        try {
            return getConnectionPool().getConnection();
        }
        catch (Exception e) {
            throw new RuntimeException("exception while getting connection from pool, " + poolName, e);
        }
    }

    public Policy getPolicy() {
        return policy;
    }

    public void setPolicy(Policy policy) {
        this.policy = policy;
    }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public List<String> getHostNameList() {
        return hostNameList;
    }

    public void setHostNameList(List<String> hostNameList) {
        this.hostNameList = hostNameList;
    }

    public boolean isNodeDiscovery() {
        return nodeDiscovery;
    }

    public void setNodeDiscovery(boolean nodeDiscovery) {
        this.nodeDiscovery = nodeDiscovery;
    }

    public int getSocketReadTimeout() {
        return socketReadTimeout;
    }

    public void setSocketReadTimeout(int connTimeout) {
        this.socketReadTimeout = connTimeout;
    }

    public boolean isThriftFramedTransport() {
        return thriftFramedTransport;
    }

    public void setThriftFramedTransport(boolean thriftFramedTransport) {
        this.thriftFramedTransport = thriftFramedTransport;
    }

    public void setGeneralPolicy(GeneralPolicy generalPolicy) {
        this.generalPolicy = generalPolicy;
    }

}
