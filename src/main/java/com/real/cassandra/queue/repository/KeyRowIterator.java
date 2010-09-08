package com.real.cassandra.queue.repository;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

public class KeyRowIterator {
    public void doIt(String poolName, String colFamName, int maxKeyCount, ConsistencyLevel consistencyLevel,
            KeyOperator op) throws Exception {
        Selector s = Pelops.createSelector(poolName);
        SlicePredicate pred = Selector.newColumnsPredicateAll(false, 100);
        Bytes startKey = Bytes.fromBytes(new byte[] {});
        Bytes finishKey = Bytes.fromBytes(new byte[] {});
        while (true) {
            // List<Column> colList = s.getColumnsFromRow(colFamName, rowKey,
            // pred, consistencyLevel);
            KeyRange keyRange = Selector.newKeyRange(startKey, finishKey, maxKeyCount);
            Map<Bytes, List<Column>> retList = s.getColumnsFromRows(colFamName, keyRange, pred, consistencyLevel);
            boolean skipFirst = 0 < startKey.length();
            if (retList.isEmpty() || (skipFirst && 1 == retList.size())) {
                break;
            }

            Iterator<Entry<Bytes, List<Column>>> iter = retList.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<Bytes, List<Column>> entry = iter.next();
                if (skipFirst) {
                    skipFirst = false;
                    startKey = null;
                    continue;
                }

                op.execute(entry.getKey(), entry.getValue());

                startKey = entry.getKey();
            }
            //
            // if (null == lastColName || 0 == lastColName.length) {
            // break;
            // }
        }
    }

    interface KeyOperator {
        void execute(Bytes key, List<Column> colList) throws Exception;
    }
}
