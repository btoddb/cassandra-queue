rsync --progress --recursive --dirs --delete --rsh=ssh ~/cassandra-queue_lib/ cassandra@kv-app01.dev.real.com:/data/cassqueue/app/lib/
rsync --progress --rsh=ssh ~/cassandra-queue.jar cassandra@kv-app01.dev.real.com:/data/cassqueue/app/lib/.
