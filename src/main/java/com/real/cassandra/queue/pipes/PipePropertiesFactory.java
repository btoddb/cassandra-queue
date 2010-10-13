package com.real.cassandra.queue.pipes;

import java.util.UUID;

import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.thrift.Column;

public class PipePropertiesFactory {
    private static final String FIELD_DESCRIPTION = "pushStatus/popStatus/pushCount/timestamp";

    public PipeProperties createInstance(PipeStatus pushStatus, PipeStatus popStatus, int pushCount, long startTimestamp) {
        return new PipeProperties(pushStatus, popStatus, pushCount, startTimestamp);
    }

    public PipeProperties createInstance(String raw) {
        if (null == raw || raw.isEmpty()) {
            throw new IllegalArgumentException("raw string is invalid - must be '" + FIELD_DESCRIPTION + "'");
        }

        String[] tmpArr = raw.split(" */ *");
        if (4 != tmpArr.length) {
            throw new IllegalArgumentException("raw string is invalid - must be '" + FIELD_DESCRIPTION + "'");
        }

        return new PipeProperties(PipeStatus.getInstance(tmpArr[0].trim()), PipeStatus.getInstance(tmpArr[1].trim()),
                Integer.parseInt(tmpArr[2].trim()), Long.parseLong(tmpArr[3].trim()));
    }

    public String createInstance(PipeProperties ps) {
        if (null == ps || null == ps.getPushStatus() || null == ps.getPopStatus() || 0 > ps.getPushCount()) {
            throw new IllegalArgumentException("raw string is invalid - must be '" + FIELD_DESCRIPTION + "'");
        }

        return ps.getPushStatus().getName() + "/" + ps.getPopStatus().getName() + "/" + ps.getPushCount() + "/"
                + ps.getStartTimestamp();
    }

    public PipeProperties createInstance(PipeDescriptorImpl pipeDesc) {
        return new PipeProperties(pipeDesc.getPushStatus(), pipeDesc.getPopStatus(), pipeDesc.getMsgCount(),
                pipeDesc.getStartTimestamp());
    }

    public PipeProperties createInstance(Column col) {
        return createInstance(new String(col.getValue()));
    }

    public PipeProperties createInstance(HColumn<UUID, String> col) {
        return createInstance(col.getValue());
    }

    public PipeProperties createInstance(byte[] value) {
        return createInstance(new String(value));
    }
}
