#!/bin/bash
# run spark job on YARN - SNIDCapperJob

usage="Usage: snidCappingRuleJob.sh [originalTable] [resultTable] [channelType] [scanStopTime] [scanTimeWindow] [updateTimeWindow]"

# if no args specified, show usage
if [ $# -le 2 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "../$bin">/dev/null; pwd`

. ${bin}/chocolate-env.sh

ORIGINAL_TABLE=$1
RESULT_TABLE=$2
CHANNEL_TYPE=$3
SCAN_STOP_TIME=$4
SCAN_TIME_WINDOW=$5
UPDATE_TIME_WINDOW=$6

DRIVER_MEMORY=10g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=12g
EXECUTOR_CORES=3

JOB_NAME="SNIDCappingRule"

for f in $(find $bin/../conf -name '*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.cappingrules.rules.SNIDCapper \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${SPARK_JOB_CONF} \
    --conf spark.yarn.executor.memoryOverhead=8192 \
    ${bin}/../lib/chocolate-capping-rules-*.jar \
      --jobName ${JOB_NAME} \
      --mode yarn \
      --originalTable ${ORIGINAL_TABLE} \
      --resultTable ${RESULT_TABLE} \
      --channelType ${CHANNEL_TYPE} \
      --scanStopTime "${SCAN_STOP_TIME}" \
      --scanTimeWindow ${SCAN_TIME_WINDOW} \
      --updateTimeWindow ${UPDATE_TIME_WINDOW}
