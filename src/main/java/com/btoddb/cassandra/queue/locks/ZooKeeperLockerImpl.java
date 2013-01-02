package com.btoddb.cassandra.queue.locks;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.cassandra.queue.Descriptor;
import com.btoddb.cassandra.queue.zookeeper.ZooKeeperConnectionManager;
import com.btoddb.cassandra.queue.zookeeper.ZooKeeperException;

/**
 * Adapts ZooKeeper operations to provide lock/unlock behavior. Attempts to create a unique node corresponding to a
 * resource to be locked, and deletes it when released. The nodes are ephemeral and will be automatically removed
 * if the connection is lost, or session times out. The base permanent lock path is created if it doesn't already
 * exist.
 *
 */
public class ZooKeeperLockerImpl<I extends Descriptor> implements Locker<I> {

    private static Logger logger = LoggerFactory.getLogger(ZooKeeperLockerImpl.class);

    private String lockPath;
    private ZooKeeperConnectionManager zookeeperManager;
    private String connectString;
    private Integer sessionTimeout;

    private AtomicInteger lockCount = new AtomicInteger(0);
    private AtomicInteger releaseCount = new AtomicInteger(0);

    public ZooKeeperLockerImpl(String lockPath, String connectString, Integer sessionTimeout)
            throws IOException, ExecutionException, InterruptedException {

        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
        this.lockPath = lockPath;
        PathUtils.validatePath(lockPath);

        zookeeperManager = new ZooKeeperConnectionManager(connectString, sessionTimeout, sessionTimeout/2);
        zookeeperManager.start();
    }

    private String createResourcePath(I descriptor, String lockPath){
        return lockPath+"/"+descriptor.getId().toString();
    }

    @Override
    public ObjectLock<I> lock(I object) {
        String resourceName = createResourcePath(object, lockPath);
        ObjectLock<I> lock = null;

        try {

            try {
                String node = zookeeperManager.getZookeeperSession().
                        create(resourceName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                lock = new ObjectLock<I>(object, new PathLock(node));
                logger.debug("Acquired lock for object {}", object.getId());
                lockCount.incrementAndGet();

            }
            catch (KeeperException e) {
                if(KeeperException.Code.NONODE == e.code()) {
                    String[] pathNodes = lockPath.split("/");
                    createLockPath(pathNodes, pathNodes.length);
                    return lock(object);
                }
                else if(KeeperException.Code.NODEEXISTS == e.code()) {
                    logger.debug("Lock node for id {} exists; assuming someone else holds the lock", object.getId());
                }
            }
        }
        catch (ZooKeeperException e) {
            logger.warn("Lock acquire for id {} failed: {}", object.getId(), e.getMessage());
        }
        catch(KeeperException ke) {
            logger.error(String.format("Lock acquire for id %s failed with error code %s and zookeeper exception %s",
                    object.getId(), ke.code(), ke.getMessage()), ke);
        }
        catch (InterruptedException e) {
            logger.warn("Interrupted trying to acquire lock for object {}", object.getId());
            //clear thread interrupt flag
            Thread.interrupted();
        }

        return lock;
    }

    private void createLockPath(String[] pathNodes, int pathNodesIdx) throws InterruptedException, ZooKeeperException, KeeperException {

            StringBuilder currNodePath = new StringBuilder();
            for (int i=1; i<pathNodesIdx; i++) { // i=1 to skip split()'s empty node
                currNodePath.append("/");
                currNodePath.append(pathNodes[i]);
            }

            try {
                zookeeperManager.getZookeeperSession()
                        .create(currNodePath.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                if (pathNodesIdx >= pathNodes.length) {
                    logger.debug("Lock path created: "+currNodePath.toString());
                }
                else {
                    createLockPath(pathNodes, pathNodesIdx+1);
                }

            }
            catch(KeeperException e) {
                if (e.code() == KeeperException.Code.NODEEXISTS) {
                    if (pathNodesIdx < pathNodes.length) {
                        createLockPath(pathNodes, pathNodesIdx+1);
                    }
                }
                else if(e.code() == KeeperException.Code.NONODE) {
                    createLockPath(pathNodes, pathNodesIdx-1);
                }
                else {
                    throw e;
                }
            }
    }

    @Override
    public ObjectLock<I> lock(I object, int lockRetryLimit, long lockRetryDelay) {
        ObjectLock<I> lock = null;

        for (int i = 0; i < lockRetryLimit && lock == null; i++) {
            lock = lock(object);
            if (lock == null) {
                try {
                    Thread.sleep(lockRetryDelay);
                }
                catch (InterruptedException e) {
                    logger.warn("Interrupted sleeping for lock retry.");
                    //clear thread interrupt flag
                    Thread.interrupted();
                }
            }
        }

        return lock;
    }

    @Override
    public void release(ObjectLock<I> objectLock) {

        if(objectLock != null) {
            I object = objectLock.getLockedObj();
            String resourcePath = createResourcePath(object, lockPath);
            try {

                zookeeperManager.getZookeeperSession().delete(resourcePath, -1);
                releaseCount.incrementAndGet();
                logger.debug("Released lock for object {}", objectLock.getLockedObj().getId());
            }
            catch (ZooKeeperException zke) {
                logger.warn("Lock release for id {} failed: {}", object.getId(), zke.getMessage());
            }
            catch(KeeperException ke) {
                if(ke.code() == KeeperException.Code.NONODE) {
                    logger.warn("Lock release for id {} gave error code {}; assuming lock node already gone",
                            object.getId(), ke.code());
                }
                else {
                    logger.error(String.format("Lock release for id %s failed with error code %s and exception %s",
                            object.getId(), ke.code(), ke.getMessage()), ke);
                }
            }
            catch (InterruptedException e) {
                logger.warn("Interrupted trying to acquire lock for object {}", object.getId());
                //clear thread interrupt flag
                Thread.interrupted();
            }

        }
    }

    @Override
    public void shutdownAndWait() {
        zookeeperManager.shutdown();
    }

    public int getLockCountSuccess() {
        return lockCount.get();
    }

    public int getReleaseCount() {
        return releaseCount.get();
    }

    public String getConnectString() {
        return connectString;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    public Integer getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(Integer sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

}
