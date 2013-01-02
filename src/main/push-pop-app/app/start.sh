
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
mkdir target 2> /dev/null

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

#        -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -verbose:gc \
OPTS="-Xms1G \
        -Xmx1G \
        -XX:+UseParNewGC \
        -XX:+UseConcMarkSweepGC \
        -XX:+CMSParallelRemarkEnabled \
        -XX:CMSInitiatingOccupancyFraction=88 \
	-Xdebug \
        -Dnetworkaddress.cache.ttl=60 \
	-Dcom.sun.management.jmxremote.port=9002 \
	-Dcom.sun.management.jmxremote.ssl=false \
	-Dcom.sun.management.jmxremote.authenticate=false \
	-Xrunjdwp:transport=dt_socket,server=y,address=8002,suspend=n \
"
APP_OPTS=""

java ${OPTS} -cp ${classpath} com.btoddb.cassandra.queue.PushPopApp ${APP_OPTS} > ${logDir}/console.log 2>&1 &
echo $! > ${pidFile}

popd

