package com.btoddb.cassandra.queue.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a connection to a zookeeper cluster. Does periodic checks to see if the connection is alive/disconnected,
 * and if so re-creates the connection. If requests come into the manager for a zookeeper session while it is in
 * a disconnected state, fails them fast.
 */
public class ZooKeeperConnectionManager implements Runnable, Watcher {
    private Logger logger = LoggerFactory.getLogger(ZooKeeperConnectionManager.class);

    /* manager thread parameters */
    private String threadName = "Zookeeper Connection Manager";
    private Thread t;
    private long sessionPollingInterval;

    /* used for exclusive access while modifying the "shouldRun" flag */
    private final Object shutdownMonitor = new Object();
    private volatile boolean shouldRun = true;

    /* this flag will be used internally to maintain connection state;
     * lock should be held while working with the connection object */
    private final Object connectionEstablishedMonitor = new Object();
    private boolean connectionEstablished = false;

    /* connection and creation, params */
    private ZooKeeper zkConnection;
    private String connectString;
    private Integer sessionTimeout;

    /* this flag is used to indicate whether requests to getSession should fail fast;
     * as such, lock should be held as short as possible, to prevent blocking */
    private final Object allowSessionRequestMonitor = new Object();
    private boolean allowSessionRequests = false;


    /**
     * Constructor; start() should be called on the instance after creation.
     *
     * @param connectString Zookeeper connect string
     * @param sessionTimeout zookeeper session timeout
     * @param sessionPollingInterval frequency in ms that manager thread should check for session availability
     */
    public ZooKeeperConnectionManager(String connectString, Integer sessionTimeout, long sessionPollingInterval) {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
        this.sessionPollingInterval = sessionPollingInterval;
        t = new Thread(this);
    }


	/**
	 * Get a new {@link ZooKeeper} client.
     *
	 * @return new client session
     * @throws ZooKeeperException in event of an error with session or underlying connection
	 */
    public ZooKeeper getZookeeperSession() throws ZooKeeperException {
        //ensure the
        synchronized(allowSessionRequestMonitor) {
            if(!allowSessionRequests || zkConnection == null){

                throw new ZooKeeperException("Connection not available.");
            }
            else {

                return zkConnection;
            }
        }
    }


    /**
     * Creates a Zookeeper connection.
     *
     * @throws ZooKeeperException in event of an error during connection creation
     */
    private void createConnection() throws IOException, ZooKeeperException {
        //ensure session requests will not be incoming
        synchronized(allowSessionRequestMonitor){
            allowSessionRequests = false;
        }

        //create the connection
        synchronized (connectionEstablishedMonitor) {

            logger.info("Creating connection object.");
            zkConnection = new ZooKeeper(connectString, sessionTimeout, this);

            postProcessCreateConnection(zkConnection);
        }


    }

    /**
     * performs any post-tasks that should be done on connection creation.
     *
     * @param zooKeeper  connection to process
     * @throws ZooKeeperException in event of a problem
     */
    protected void postProcessCreateConnection(ZooKeeper zooKeeper) throws ZooKeeperException {

    }

    /**
     * close then re-create the connection
     */
    private void resetConnection() {
        try {
            // close the previous connection
            closeConnection();
            // create new connection
            createConnection();
        }
        catch (Exception ex) {
            logger.error("resetting connection failed: " + ex.getMessage());
        }

    }


    /**
     * stop any pending connection activities, then close
     */
    private void closeConnection() {

        //ensure no new requests will be incoming
        synchronized (allowSessionRequestMonitor) {
            allowSessionRequests = false;
        }

        synchronized (connectionEstablishedMonitor) {
            if (zkConnection != null && zkConnection.getState().isAlive()) {
                try {
                    logger.info("Closing connection object.");
                    zkConnection.close();
                    zkConnection = null;
                }
                catch (Exception ex) {
                    logger.warn("Closing connection failed: " + ex.getMessage());
                }
            }

           connectionEstablished = false;
        }

    }

    /**
     * Indicate that there was a problem detected with the underlying connection, and is considered disconnected
     */
    protected void signalDisconnected() {

        synchronized (allowSessionRequestMonitor) {
            allowSessionRequests = false;
        }

        synchronized (connectionEstablishedMonitor) {
            connectionEstablished = false;
        }
    }

    /**
     * Indicates we are now connected to zookeeper and should allow client requests.
     */
    protected void signalConnected() {
        synchronized (connectionEstablishedMonitor) {
            connectionEstablished = true;
        }

        synchronized (allowSessionRequestMonitor) {
            allowSessionRequests = true;
        }
    }


    public void start() {
        t.setName(threadName);
        t.start();
    }

    private void init() {
        try {
            createConnection();
        }
        catch (Exception ex) {
            logger.error("Error creating connection:" + ex.getMessage());
        }

    }

    public void shutdown() {
        synchronized (shutdownMonitor) {
            shouldRun = false;
        }

        logger.info("Shutdown signaled, waiting for manager thread to finish..");

        try {
            int sleepCount = 0;
            //wait for manager thread to finish
            while(t.isAlive()) {

                //if we've already waited longer than twice the polling interval, interrupt the thread
                if(sleepCount*100 > 2*sessionPollingInterval) {
                    t.interrupt();
                    break;
                }

                //sleep for 100ms
                Thread.sleep(100);
                sleepCount++;
            }

        } catch (InterruptedException ex) {
            logger.error("Interrupted waiting for session monitor thread to shutdown : " + ex.getMessage());
        }
    }


    protected void onConnected() {
        logger.warn("ZooKeeper connection established.");
        signalConnected();
    }

    protected void onDisconnection() {
        logger.warn("ZooKeeper connection broken.");
        signalDisconnected();
    }

    protected void onSessionExpired() {
        logger.warn("ZooKeeper connection expired; reconnection required");
        signalDisconnected();
    }

    /**
     * Handle incoming Zookeeper client notifications.
     * @param event check for appropriate action
     */
    @Override
    public void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.None) {
    		Event.KeeperState state = event.getState();
    		switch (state) {
                case SyncConnected:
                    onConnected();
                    break;
                case Disconnected:
                    onDisconnection();
                    break;
                case Expired:
                    onSessionExpired();
                    break;
                }
		}
    }


    public void run() {
        init();
        try {
            while (true) {
                //sleep first to give any new connection objects a chance to be established and call-back onConnected()
                Thread.sleep(sessionPollingInterval);

                synchronized (shutdownMonitor) {
                    if (!shouldRun) {
                        break;
                    }
                }

                //check session aliveness
                try {

                    if (getZookeeperSession().getState() == null ||
                            !Boolean.TRUE.equals(getZookeeperSession().getState().isAlive())) {

                        logger.warn("Dead session returned, assuming disconnected");
                        signalDisconnected();
                    }
                    else {
                        logger.debug("Session indicates it is still alive.");
                    }
                }
                catch (Exception e) {
                    logger.error(String.format("Exception creating session [%s]: %s",
                            e.getClass().getSimpleName(),
                            e.getMessage()));
                    signalDisconnected();
                }

                //check for need to reconnect
                synchronized (connectionEstablishedMonitor) {
                    if (!connectionEstablished) {
                        resetConnection();
                    }
                }
            }

        }
        catch (InterruptedException e) {
            logger.warn("Thread interrupted, shutting down.", e);
        }
        catch (Exception e) {
            logger.error("Unhandled exception caught, thread exiting.", e);
        }
        finally {
            //close connection
            closeConnection();
        }

        logger.info("Connection monitor thread ended.");
    }

}

