package com.real.cassandra.queue.repository;

import java.util.List;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

public class ColumnIterator {
    private static final byte[] EMPTY_BYTES = new byte[] {};
    private int maxColsPerPage = 500;

	public void doIt(Cluster cluster, String keyspaceName, String colFamName, byte[] rowKey, ColumnOperator op) {
        doIt(cluster, keyspaceName, colFamName, rowKey, null, op);
    }

    public void doIt(Cluster cluster, String keyspaceName, String colFamName, byte[] rowKey,
            byte[] lastColName, ColumnOperator op) {
        BytesArraySerializer bs = BytesArraySerializer.get();
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        SliceQuery<byte[], byte[], byte[]> sliceQuery = HFactory.createSliceQuery(keyspace, bs, bs, bs);
        sliceQuery.setColumnFamily(colFamName);
        sliceQuery.setKey(rowKey);

        if (null == lastColName) {
            lastColName = EMPTY_BYTES;
        }

        while (true) {
            sliceQuery.setRange(lastColName, null, false, maxColsPerPage + 1);
            QueryResult<ColumnSlice<byte[], byte[]>> result = sliceQuery.execute();

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

    public void setMaxColsPerPage(int maxColsPerPage) {
		this.maxColsPerPage = maxColsPerPage;
	}
}
