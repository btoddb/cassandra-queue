package com.real.cassandra.queue;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;

import com.real.cassandra.queue.repository.QueueRepositoryImpl;
import com.real.cassandra.queue.utils.DoubleSerializer;

public class QueueStatsFactoryImpl {

    public QueueStats createInstance(String qName, long totalPushes, long totalPops, double recentPushesPerSec,
            double recentPopsPerSec) {
        QueueStats qStats = new QueueStats(qName);
        qStats.setTotalPushes(totalPushes);
        qStats.setTotalPops(totalPops);
        qStats.setRecentPushesPerSec(recentPushesPerSec);
        qStats.setRecentPopsPerSec(recentPopsPerSec);
        return qStats;
    }

    public QueueStats createInstance(String qName, ColumnSlice<String, byte[]> colSlice) {
        if (colSlice.getColumns().isEmpty()) {
            return null;
        }

        QueueStats qStats = new QueueStats(qName);
        qStats.setTotalPushes(LongSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.QSTATS_COLNAME_TOTAL_PUSHES).getValue()));
        qStats.setTotalPops(LongSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.QSTATS_COLNAME_TOTAL_POPS).getValue()));
        qStats.setRecentPushesPerSec(DoubleSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.QSTATS_COLNAME_RECENT_PUSHES_PER_SEC).getValue()));
        qStats.setRecentPopsPerSec(DoubleSerializer.get().fromBytes(
                colSlice.getColumnByName(QueueRepositoryImpl.QSTATS_COLNAME_RECENT_POPS_PER_SEC).getValue()));
        return qStats;
    }

}
