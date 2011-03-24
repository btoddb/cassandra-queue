if [ $# != 1 ] ; then
  echo
  echo "usage: $0 <version>"
  echo

  exit
fi

version=$1
remoteUserName=cassandra
remoteBasePath="/data/cassqueue"
remoteAppPath="${remoteBasePath}/app"
remoteConfPath="${remoteBasePath}/conf"
localBaseDir=`dirname $0`
tarFile=cassandra-queue-${version}.tar.gz
hostName="kv-app01.dev.real.com"
explodeDir=target/explode.tmp

#
# set working directory
#

pushd ${localBaseDir}

#
# make required remote dirs
#

ssh ${remoteUserName}@${hostName} "mkdir -p ${remoteAppPath} ${remoteConfPath} 2> /dev/null"

#
# explode tar and use rsync to properly add/remove files
#

rm -rf ${explodeDir} 2> /dev/null
mkdir -p ${explodeDir}
tar xvfz target/${tarFile} -C ${explodeDir}

#
# sync the files
#

# libs
rsync --progress --recursive --dirs --delete --rsh=ssh ${explodeDir}/app ${remoteUserName}@${hostName}:${remoteBasePath}/
rsync --progress --recursive --cvs-exclude --dirs --delete --rsh=ssh ${explodeDir}/conf ${remoteUserName}@${hostName}:${remoteBasePath}/

#
# return to starting state
#

rm -r ${explodeDir}

popd
