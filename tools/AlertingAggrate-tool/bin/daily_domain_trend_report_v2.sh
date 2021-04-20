#!/usr/bin/env bash
export HADOOP_USER_NAME=hdfs

DATE=`date --date= +%Y-%m-%d`

DRIVER_MEMORY=6g
EXECUTOR_NUMBER=40
EXECUTOR_MEMORY=8g
EXECUTOR_CORES=5
SPARK_HOME=/datashare/mkttracking/tools/hercules_lvs/spark-hercules/

/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -mkdir hdfs://hercules/apps/b_marketing_tracking/alert/epn/$DATE
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -rm -r hdfs://hercules/apps/b_marketing_tracking/alert/epn/$DATE/dailyDomainTrend

${SPARK_HOME}/bin/spark-submit  \
    --files ${FILES}  \
    --class com.ebay.traffic.chocolate.job.AmsClickReport_v2 \
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
    /datashare/mkttracking/tools/AlertingAggrate-tool/lib/AlertingAggrate-tool-*.jar hdfs://hercules/sys/edw/imk/im_tracking/epn/ams_click/snapshot/ hdfs://hercules/apps/b_marketing_tracking/alert/epn/$DATE/dailyDomainTrend dailyDomainTrend yarn
