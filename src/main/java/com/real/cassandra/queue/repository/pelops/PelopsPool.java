package com.real.cassandra.queue.repository.pelops;

import java.security.InvalidParameterException;

import org.scale7.cassandra.pelops.CachePerNodePool.Policy;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IThriftPool;
import org.scale7.cassandra.pelops.OperandPolicy;
import org.scale7.cassandra.pelops.Pelops;

public class PelopsPool {
    private Cluster cluster;
    private Policy policy = new Policy();
    private OperandPolicy operandPolicy = new OperandPolicy();
    private String poolName = "defaultPool";
    private String keyspaceName;
    private boolean nodeDiscovery = false;
    private int socketReadTimeout = 5000;

    public void initPool() {
        if (null == cluster) {
            throw new InvalidParameterException("cluster has not been set");
        }

        if (null == policy) {
            throw new InvalidParameterException("policy property has not been set");
        }
        Pelops.addPool(poolName, cluster, keyspaceName, operandPolicy, policy);
    }

    public IThriftPool getConnectionPool() {
        return Pelops.getDbConnPool(poolName);
    }

    public IThriftPool.IConnection getConnection() {
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

    public void setOperandPolicy(OperandPolicy operandPolicy) {
        this.operandPolicy = operandPolicy;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

}
