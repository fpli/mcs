#!/usr/bin/env bash
export HADOOP_USER_NAME=hdfs

DATE=`date --date= +%Y-%m-%d`

DRIVER_MEMORY=6g
EXECUTOR_NUMBER=40
EXECUTOR_MEMORY=8g
EXECUTOR_CORES=5
SPARK_HOME=/datashare/mkttracking/tools/apache/spark
FILES=/datashare/mkttracking/tools/AlertingAggrate-tool/conf/df_epn_click.json

hdfs dfs -mkdir hdfs://elvisha/apps/alert/epn/$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/alert/epn/$DATE/dailyClickTrend

${SPARK_HOME}/bin/spark-submit  \
    --files ${FILES}  \
    --class com.ebay.traffic.chocolate.job.AmsClickReport \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    --queue default \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.hadoop.yarn.timeline-service.enabled=false \
    --conf spark.sql.autoBroadcastJoinThreshold=33554432 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.speculation=false \
    --conf spark.yarn.maxAppAttempts=3 \
    --conf spark.driver.maxResultSize=10g \
    --conf spark.kryoserializer.buffer.max=2040m \
    --conf spark.task.maxFailures=3 \
    /datashare/mkttracking/tools/AlertingAggrate-tool/lib/AlertingAggrate-tool-3.4.2-RELEASE-fat.jar hdfs://elvisha/apps/epn-nrt/click/ hdfs://elvisha/apps/alert/epn/$DATE/dailyClickTrend dailyClickTrend df_epn_click.json yarn
