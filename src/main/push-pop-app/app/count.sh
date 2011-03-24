
appDir=`dirname $0`
homeDir="${appDir}/.."
libDir=${appDir}/lib
pidFile=${appDir}/app.pid
logDir=log
confDir=conf

#
# setup app env
#

pushd ${homeDir}
mkdir ${logDir} 2> /dev/null

#
# needed by YourKit profiler
#

###export LD_LIBRARY_PATH=~/yourkit/lib:${LD_LIBRARY_PATH}
###	-agentlib:yjpagent=disablej2ee,noj2ee,port=8998 \

#
# setup CLASSPATH
#

classpath=${confDir}
for lib in `ls -1 ${libDir}/*` ; do
  classpath=${classpath}:${lib}
done

#	-Xrunjdwp:transport=dt_socket,server=y,address=8000,suspend=n \
OPTS="-Xms1G \
        -Xmx1G \
        -XX:+UseParNewGC \
        -XX:+UseConcMarkSweepGC \
        -XX:+CMSParallelRemarkEnabled \
        -XX:CMSInitiatingOccupancyFraction=88 \
        -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -verbose:gc \
	-Xdebug \
        -Dnetworkaddress.cache.ttl=60 \
	-Dcom.sun.management.jmxremote.port=9001 \
	-Dcom.sun.management.jmxremote.ssl=false \
	-Dcom.sun.management.jmxremote.authenticate=false \
"
APP_OPTS="--count"
#APP_OPTS="--dump-pipe 3ab8cad1-bb49-11df-8d33-95186c4f1831"

java ${OPTS} -cp ${classpath} com.real.cassandra.queue.app.CassQueueApp ${APP_OPTS}
echo $! > ${pidFile}

popd

