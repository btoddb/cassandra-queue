rsync --progress --recursive --dirs --delete --rsh=ssh /btoddb/home/bburruss/cassandra-queue_lib/ cassandra@kv-app01.dev.real.com:/data/cassqueue/app/lib/
rsync --progress --rsh=ssh /btoddb/home/bburruss/cassandra-queue.jar cassandra@kv-app01.dev.real.com:/data/cassqueue/app/lib/.
