package com.real.cassandra.queue.pipes;

import java.util.UUID;

import me.prettyprint.cassandra.model.HColumn;

import org.apache.cassandra.thrift.Column;

public class PipeStatusFactory {

    public PipeStatus createInstance(String status, int pushCount) {
        return new PipeStatus(status, pushCount);
    }

    public PipeStatus createInstance(String raw) {
        if (null == raw || raw.isEmpty()) {
            throw new IllegalArgumentException("raw string is invalid - must be 'status/pushCount'");
        }

        String[] tmpArr = raw.split(" */ *");
        if (2 != tmpArr.length) {
            throw new IllegalArgumentException("raw string is invalid - must be 'status/pushCount'");
        }

        return new PipeStatus(tmpArr[0].trim(), Integer.parseInt(tmpArr[1].trim()));
    }

    public String createInstance(PipeStatus ps) {
        if (null == ps || ps.getStatus().isEmpty() || 0 > ps.getPushCount()) {
            throw new IllegalArgumentException("raw string is invalid - must be 'status/pushCount'");
        }

        return ps.getStatus() + "/" + ps.getPushCount();
    }

    public PipeStatus createInstance(Column col) {
        return createInstance(new String(col.getValue()));
    }

    public PipeStatus createInstance(HColumn<UUID, String> col) {
        return createInstance(col.getValue());
    }
}
