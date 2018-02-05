#!/bin/bash
# chocolate-env.sh

if [ -z "${HADOOP_HOME}" ]; then
  export HADOOP_HOME=/usr/hdp/current/hadoop-client
fi

if [ -z "${HADOOP_CONF_DIR}" ]; then
  export HADOOP_CONF_DIR=/usr/hdp/current/hadoop-client/etc/hadoop/
fi

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME=/usr/hdp/2.5.0.0-1245/spark2
fi

if [ -z "${SPARK_CONF_DIR}" ]; then
  export SPARK_CONF_DIR=/usr/hdp/2.5.0.0-1245/spark2/conf
fi

SPARK_EVENTLOG_DIR=hdfs://choco-2024312.lvs02.dev.ebayc3.com/app-logs/spark/logs
HISTORY_SERVER=http://choco-2024115.lvs02.dev.ebayc3.com:18081

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