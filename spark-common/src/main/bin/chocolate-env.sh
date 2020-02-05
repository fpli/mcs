#!/bin/bash
# chocolate-env.sh

if [ -z "${HADOOP_HOME}" ]; then
  export HADOOP_HOME=/usr/hdp/current/hadoop-client
fi

if [ -z "${HADOOP_CONF_DIR}" ]; then
  export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop/
fi

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME=/datashare/mkttracking/tools/apache/spark
fi

if [ -z "${SPARK_CONF_DIR}" ]; then
  export SPARK_CONF_DIR=${SPARK_HOME}/conf/
fi

echo "Start getting cluster name."
HostName=`hostname`
echo "HostName: "$HostName
clustername=${HostName:0:3}
echo "clustername: "$clustername
if [ "$clustername" == "lvs" ]; then
    echo "The clustername is lvs"
    SPARK_EVENTLOG_DIR=hdfs://elvisha/spark-history-logs/chocolate/logs
    HISTORY_SERVER=http://lvschocolatepits-1583698.stratus.lvs.ebay.com:18080/
elif [ "$clustername" == "slc" ]; then
    echo "The clustername is slc"
    SPARK_EVENTLOG_DIR=hdfs://slickha/spark-history-logs/chocolate/logs
    HISTORY_SERVER=http://slcchocolatepits-1242733.stratus.slc.ebay.com:18080/
else
  echo "The clustername is not slc or lvs."
fi
echo "SPARK_EVENTLOG_DIR: "$SPARK_EVENTLOG_DIR
echo "HISTORY_SERVER: "$HISTORY_SERVER
echo "Finish getting cluster name."

FILES="file:///${HADOOP_CONF_DIR}/ssl-client.xml"

QUEUE_NAME=default
SPARK_YARN_MAX_APP_ATTEMPTS=3
SPARK_TASK_MAX_FAILURES=3

read -d '' SPARK_JOB_CONF << EOF
    --queue ${QUEUE_NAME} \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.hadoop.yarn.timeline-service.enabled=false \
    --conf spark.sql.autoBroadcastJoinThreshold=33554432 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.speculation=false \
    --conf spark.yarn.maxAppAttempts=${SPARK_YARN_MAX_APP_ATTEMPTS} \
    --conf spark.driver.maxResultSize=10g \
    --conf spark.kryoserializer.buffer.max=2040m \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.compress=false \
    --conf spark.yarn.historyServer.address=${HISTORY_SERVER} \
    --conf spark.eventLog.dir=${SPARK_EVENTLOG_DIR} \
    --conf spark.task.maxFailures=${SPARK_TASK_MAX_FAILURES}
EOF
