#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

if [ -z "${JAVA_HOME}" ]; then
  export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/
fi

if [ -z "${HADOOP_HOME}" ]; then
  export HADOOP_HOME=/usr/hdp/2.6.0.3-8/hadoop
fi

if [ -z "${FLUME_HOME}" ]; then
  export FLUME_HOME=/home/hdfs/flume-1.8.0
fi

${FLUME_HOME}/bin/flume-ng agent --conf ${FLUME_HOME}/conf --conf-file ${bin}/../../conf/flume_lvs.conf --classpath ${bin}/../../lib/ingester-*.jar --name a1 -Dflume.root.logger=INFO,console -Xloggc:${bin}/../../logs/ingester.log