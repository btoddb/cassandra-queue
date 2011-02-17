package com.real.cassandra.queue.locks;

/**
 * Simple lock object representing a locked path. Doesn't hold any locking behavior.
 *
 * @author Andrew Ebaugh
 */
public class PathLock implements Lock {

    private String path;

    public PathLock(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    @Override
    public void release() {

    }
}
