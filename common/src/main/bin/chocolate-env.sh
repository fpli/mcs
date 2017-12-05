#!/bin/bash
# chocolate-env.sh

if [ -z "${HADOOP_HOME}" ]; then
  export HADOOP_HOME=/usr/hdp/current/hadoop-client
fi

if [ -z "${HADOOP_CONF_DIR}" ]; then
  export HADOOP_CONF_DIR=/usr/hdp/current/hadoop-client/etc/hadoop/
fi

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME=/apache/spark/
fi

if [ -z "${SPARK_CONF_DIR}" ]; then
  export SPARK_CONF_DIR=/apache/spark/conf/
fi

SPARK_EVENTLOG_DIR=hdfs://lvschocolatemaster-1448895.stratus.lvs.ebay.com:8020/app-logs/spark/logs
HISTORY_SERVER=http://lvschocolatepits-1583698.stratus.lvs.ebay.com:18080/

FILES="file:///${SPARK_CONF_DIR}/hbase-site.xml,file:///${HADOOP_CONF_DIR}/ssl-client.xml"

QUEUE_NAME=default
SPARK_YARN_MAX_APP_ATTEMPTS=3
SPARK_TASK_MAX_FAILURES=3

read -d '' SPARK_JOB_CONF << EOF
    --queue ${QUEUE_NAME} \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.hadoop.yarn.timeline-service.enabled=false \
    --conf spark.sql.autoBroadcastJoinThreshold=536870912 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.speculation=false \
    --conf spark.yarn.maxAppAttempts=${SPARK_YARN_MAX_APP_ATTEMPTS} \
    --conf spark.driver.maxResultSize=10g \
    --conf spark.kryoserializer.buffer.max=2040m \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=${SPARK_EVENTLOG_DIR} \
    --conf spark.eventLog.compress=false \
    --conf spark.yarn.historyServer.address=${HISTORY_SERVER} \
    --conf spark.task.maxFailures=${SPARK_TASK_MAX_FAILURES}
EOF
