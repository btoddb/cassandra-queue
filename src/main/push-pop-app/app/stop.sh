baseDir=`dirname $0`
pidFile=app.pid

pushd ${baseDir}

kill `cat ${pidFile}`

popd
