#!/bin/bash
# run spark job on YARN - ReportDataGeneratorJob

usage="Usage: reportGeneratorJob.sh [originalTable] [resultTable] [channelType] [scanStopTime] [scanTimeWindow] [storageType] [env]"

# if no args specified, show usage
if [ $# -le 2 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "../$bin">/dev/null; pwd`

. ${bin}/chocolate-env-qa.sh

ORIGINAL_TABLE=$1
RESULT_TABLE=$2
CHANNEL_TYPE=$3
SCAN_STOP_TIME=$4
SCAN_TIME_WINDOW=$5
STORAGE_TYPE=$6
ENV=$7

DRIVER_MEMORY=1g
EXECUTOR_NUMBER=2
EXECUTOR_MEMORY=512M
EXECUTOR_CORES=3

JOB_NAME="ReportDataGenerator"

for f in $(find $bin/../conf -name '*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.cappingrules.cassandra.ReportDataGenerator \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${SPARK_JOB_CONF} \
    --conf spark.yarn.executor.memoryOverhead=1024 \
    ${bin}/../lib/chocolate-capping-rules-*.jar \
      --jobName ${JOB_NAME} \
      --mode yarn \
      --originalTable ${ORIGINAL_TABLE} \
      --resultTable ${RESULT_TABLE} \
      --channelType ${CHANNEL_TYPE} \
      --scanStopTime "${SCAN_STOP_TIME}" \
      --scanTimeWindow ${SCAN_TIME_WINDOW} \
      --storageType ${STORAGE_TYPE} \
      --env ${ENV} \
