#!/usr/bin/env bash

NANOTIMESTAMP=`date +%s%N`
TIMESTAMP=$(($NANOTIMESTAMP/1000000))
ESURL=http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200
export HADOOP_USER_NAME=hdfs

echo $HADOOP_USER_NAME
echo $TIMESTAMP
echo $ESURL

. ./chocolate-env.sh

SPARK_EVENTLOG_DIR=hdfs://elvisha/app-logs/chocolate/logs/tools

${SPARK_HOME}/bin/spark-submit \
    --class com.ebay.traffic.chocolate.job.CheckJob \
    --master yarn \
    --deploy-mode cluster \
    ${SPARK_JOB_CONF} \
    --conf spark.eventLog.dir=${SPARK_EVENTLOG_DIR} \
    --files /home/chocolate/tools/CheckRecover-tool/conf/tasks.xml \
        /home/chocolate/tools/CheckRecover-tool/lib/CheckRecover-tool-*.jar \
        --appName "CheckJob" \
        --mode "yarn" \
        --countDataDir "" \
        --ts $TIMESTAMP \
        --taskFile "tasks.xml" \
        --elasticsearchUrl $ESURL