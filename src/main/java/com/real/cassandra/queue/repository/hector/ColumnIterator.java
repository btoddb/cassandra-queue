package com.real.cassandra.queue.repository.hector;

import java.util.List;

import me.prettyprint.cassandra.model.ColumnSlice;
import me.prettyprint.cassandra.model.HColumn;
import me.prettyprint.cassandra.model.KeyspaceOperator;
import me.prettyprint.cassandra.model.Result;
import me.prettyprint.cassandra.model.SliceQuery;
import me.prettyprint.cassandra.serializers.BytesSerializer;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

public class ColumnIterator {
    private static final byte[] EMPTY_BYTES = new byte[] {};;
    private int maxColsPerPage = 1000;

    public void doIt(Cluster cluster, String keyspaceName, String colFamName, byte[] rowKey, ColumnOperator op) {
        BytesSerializer bs = BytesSerializer.get();
        KeyspaceOperator ko = HFactory.createKeyspaceOperator(keyspaceName, cluster);
        SliceQuery<byte[], byte[], byte[]> sliceQuery = HFactory.createSliceQuery(ko, bs, bs, bs);
        sliceQuery.setColumnFamily(colFamName);
        sliceQuery.setKey(rowKey);

        byte[] lastColName = EMPTY_BYTES;
        while (true) {
            sliceQuery.setRange(lastColName, null, false, maxColsPerPage + 1);
            Result<ColumnSlice<byte[], byte[]>> result = sliceQuery.execute();

            List<HColumn<byte[], byte[]>> colList = result.get().getColumns();
            boolean skipFirst = 0 < lastColName.length;
            if (colList.isEmpty() || (skipFirst && 1 == colList.size())) {
                break;
            }

            for (HColumn<byte[], byte[]> col : colList) {
                if (skipFirst) {
                    skipFirst = false;
                    lastColName = null;
                    continue;
                }

                if (!op.execute(col)) {
                    return;
                }

                lastColName = col.getName();
            }
        }
    }

    public interface ColumnOperator {
        boolean execute(HColumn<byte[], byte[]> col);
    }
}
