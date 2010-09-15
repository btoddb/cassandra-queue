package com.real.cassandra.queue.repository.pelops;

import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

public class ColumnIterator {
    private int maxColsPerPage = 1000;

    public void doIt(String poolName, String colFamName, Bytes rowKey, ConsistencyLevel consistencyLevel,
            ColumnOperator op) throws Exception {
        Selector s = Pelops.createSelector(poolName);

        byte[] lastColName = new byte[] {};
        while (true) {
            SlicePredicate pred =
                    Selector.newColumnsPredicate(Bytes.fromBytes(lastColName), Bytes.fromBytes(new byte[] {}), false,
                            maxColsPerPage + 1);
            List<Column> colList = s.getColumnsFromRow(colFamName, rowKey, pred, consistencyLevel);
            boolean skipFirst = 0 < lastColName.length;
            if (colList.isEmpty() || (skipFirst && 1 == colList.size())) {
                break;
            }

            Iterator<Column> iter = colList.iterator();
            while (iter.hasNext()) {
                Column col = iter.next();
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
        boolean execute(Column col) throws Exception;
    }
}
