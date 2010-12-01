package org.wyki.zookeeper.cages;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.PathUtils;

/**
 * Adapted from <code>org.wyki.zookeeper.cages.ZkLockBase</code> class, originally written by {@author dominicwilliams}.
 * It is streamlined to perform fewer zookeeper operation, and simply tries to create an ephemeral lock path, then
 * delete it on release.
 * If a lock is already held then we fail fast and don't wait. That is preferred in situations where we'd like to move
 * onto the next resource to lock, rather than wait.
 *
 */
public abstract class ZkResourceWriteLock extends ZkSyncPrimitive implements ISinglePathLock {

    private String lockPath;
    private String resourcePath;
	private Object context;
	private ILockListener listener;
	private boolean tryAcquireOnly;
	private volatile LockState lockState;
	private final Object mutex = new Object();

	public ZkResourceWriteLock(String lockPath, String resourceName) {
		super(ZkSessionManager.instance());
		PathUtils.validatePath(lockPath);
		lockState = LockState.Idle;
        this.lockPath = lockPath;
        this.resourcePath = lockPath + "/" + resourceName + "-" + getType();
	}

	/** {@inheritDoc} */
	@Override
	public void acquire() throws ZkCagesException, InterruptedException {
		setLockState(LockState.Waiting);
		createLockNode();
		waitSynchronized();
	}

	/** {@inheritDoc} */
	@Override
	public void acquire(ILockListener listener, Object context) throws ZkCagesException, InterruptedException {
		setLockState(LockState.Waiting);
		this.listener = listener;
		this.context = context;
		addUpdateListener(reportStateUpdatedToListener, false);
		addDieListener(reportDieToListener);
		createLockNode();
	}

	/** {@inheritDoc} */
	@Override
	public boolean tryAcquire() throws ZkCagesException, InterruptedException {
		setLockState(LockState.Waiting);
		tryAcquireOnly = true;
		createLockNode();
		waitSynchronized();
		return lockState == LockState.Acquired;
	}

	/** {@inheritDoc} */
	@Override
	public void tryAcquire(ITryLockListener listener, Object context) throws ZkCagesException, InterruptedException {
		setLockState(LockState.Waiting);
		this.listener = listener;
		this.context = context;
		tryAcquireOnly = true;
		addUpdateListener(reportStateUpdatedToListener, false);
		addDieListener(reportDieToListener);
		createLockNode();
	}

	/** {@inheritDoc} */
	@Override
	public LockType getType() {
		return LockType.Write;
	}

	/** {@inheritDoc} */
	@Override
	public void release() {
		safeLockState(LockState.Released);
	}

	/** {@inheritDoc} */
	@Override
	public LockState getState()
	{
		return lockState;
	}

	/**
	 * What path does this instance lock?
	 * @return								The path that has/will be locked by this lock instance
	 */
	@Override
	public String getLockPath() {
		return resourcePath;
	}

	private void createLockNode() throws InterruptedException {
        createLockNode.run();
	}


	@Override
	protected void onDie(ZkCagesException killerException) {
		// We just set the lock state. The killer exception has already been set by base class
		safeLockState(LockState.Error);
	}

	private Runnable createLockNode = new Runnable () {

		@Override
		public void run() {
			zooKeeper().create(resourcePath, new byte[0],
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createLockNodeHandler, this);
		}

	};

	private StringCallback createLockNodeHandler = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
            if(passOrTryRepeat(rc, new Code[] { Code.OK, Code.NONODE }, (Runnable)ctx)) {
                if (rc == Code.OK.intValue()) {
                    // Lock acquired
                    safeLockState(LockState.Acquired);
                }
                else if(rc == Code.NONODE.intValue()) {
                    //we need to create the persistent lock path
                    new ZkPath(lockPath, CreateMode.PERSISTENT);
                    createLockNode.run();
                }
            }
		}

	};


	private Runnable releaseLock = new Runnable () {

		@Override
		public void run() {
			zooKeeper().delete(resourcePath, -1, releaseLockHandler, this);
		}

	};

	private VoidCallback releaseLockHandler = new VoidCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx) {
			passOrTryRepeat(rc, new Code[] { Code.OK, Code.NONODE }, (Runnable)ctx);
		}

	};

	private Runnable reportStateUpdatedToListener = new Runnable() {

		@Override
		public void run() {
			if (tryAcquireOnly && lockState != LockState.Acquired) {
				// We know that an error has not occurred, because that is passed to handler below. So report attempt
				// to acquire locked failed because was already held.
				ITryLockListener listener = (ITryLockListener) ZkResourceWriteLock.this.listener;
				listener.onTryAcquireLockFailed(ZkResourceWriteLock.this, context);
			}
			else
				listener.onLockAcquired(ZkResourceWriteLock.this, context);
		}

	};

	private Runnable reportDieToListener = new Runnable() {

		@Override
		public void run() {
			listener.onLockError(getKillerException(), ZkResourceWriteLock.this, context);
		}

	};

	/**
	 * Set the lock state when we know an exception can't be thrown
	 * @param newState					The new lock state
	 */
	private void safeLockState(LockState newState) {
		try {
			setLockState(newState);
		} catch (ZkCagesException e) {
			e.printStackTrace();
			assert false : "Unknown condition";
		}
	}

	/**
	 * Set the lock state
	 * @param newState					The new lock state
	 * @throws ZkCagesException
	 */
	private void setLockState(LockState newState) throws ZkCagesException {

		synchronized (mutex) {
			switch (newState) {

			case Idle:
				assert false : "Unknown condition";

			case Waiting:
				/**
				 * We only set this state from the public interface methods. This means we can directly throw an
				 * exception back at the caller!
				 */
				switch (lockState) {
				case Idle:
					// Caller is starting operation
					lockState = newState;
					return;
				case Waiting:
					throw new ZkCagesException(ZkCagesException.Error.LOCK_ALREADY_WAITING);
				case Abandoned:
					throw new ZkCagesException(ZkCagesException.Error.LOCK_ALREADY_ABANDONED);
				case Acquired:
					throw new ZkCagesException(ZkCagesException.Error.LOCK_ALREADY_ACQUIRED);
				case Released:
					throw new ZkCagesException(ZkCagesException.Error.LOCK_ALREADY_RELEASED);
				default:
					assert false : "Unknown condition";
				}
				break;

			case Abandoned:
				/**
				 * We tried to acquire a lock, but it was already held and we are abandoning our attempt to acquire.
				 */
				switch (lockState) {
				case Waiting:
					// Attempt to acquire lock without blocking has failed
					lockState = newState;
					// Release our lock node immediately
					releaseLock.run();
					// Notify listeners about result
					if (listener != null) {
						ITryLockListener listener = (ITryLockListener)this.listener;
						listener.onTryAcquireLockFailed(this, context);
					}
					// Notify waiting callers about result
					onStateUpdated();
					return;
				case Released:
					// Logically the lock has already been released. No node was created, so no need to releaseLock.run()
					return;
				default:
					assert false : "Unknown condition";
				}
				break;

			case Acquired:
				/**
				 * We have successfully acquired the lock.
				 */
				switch (lockState) {
				case Waiting:
					// Attempt to acquire lock has succeeded
					lockState = newState;
					// Notify caller
					onStateUpdated();
					return;
				case Released:
				case Error:
					// The lock has already been logically released or an error occurred. We initiate node release, and return
					releaseLock.run();
					return;
				default:
					assert false : "Unknown condition";
				}
				break;

			case Released:
				/**
				 * We are releasing a lock. This can be done before a lock has been acquired if an operation is in progress.
				 */
				switch (lockState) {
				case Idle:
					// Change to the released state to prevent this lock being used again
					lockState = newState;
					return;
				case Released:
				case Abandoned:
					// We consider that release() has been called vacuously
					return;
				case Waiting:
					// release() method called while waiting to acquire lock (or during the process of trying to acquire a lock).
					// This causes an error!
					die(new ZkCagesException(ZkCagesException.Error.LOCK_RELEASED_WHILE_WAITING)); // die callback will set state
					return;
				case Acquired:
					// We are simply releasing the lock while holding it. This is fine!
					lockState = newState;
					// Initiate the release procedure immediately
					releaseLock.run();
					return;
				default:
					assert false : "Unknown condition";
				}
				break;

			case Error:
				switch (lockState) {
				case Released:
					// Error is vacuous now. Lock has already been released (or else, break in session will cause ephemeral node to disappear etc)
					return;
				default:
					// ZkSyncPrimitive infrastructure is handling passing exception notification to caller, so just set state
					lockState = newState;
					return;
				}
			}

			assert false : "Unknown condition";
		}
	}
}
