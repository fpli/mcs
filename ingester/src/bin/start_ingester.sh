#!/bin/bash

usage="Usage: start_ingester.sh [dc]. dc: phx, slc, lvs."

# if no args specified, show usage
if [ $# -ne 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

dc=$1

if [ -z "${JAVA_HOME}" ]; then
  export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/
fi

if [ -z "${HADOOP_HOME}" ]; then
  export HADOOP_HOME=/usr/hdp/2.6.0.3-8/hadoop
fi

if [ -z "${FLUME_HOME}" ]; then
  export FLUME_HOME=/apache/flume-1.8.0
fi

${FLUME_HOME}/bin/flume-ng agent --conf ${FLUME_HOME}/conf --conf-file ${bin}/../conf/flume_${dc}.conf --classpath ${bin}/../lib/chocolate-ingester-*.jar --name a1