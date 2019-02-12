#!/bin/bash
# run spark job on YARN - Reporting

usage="Usage: ruleVerifier0.sh [workPath] [chocoTodayPath] [chocoYesterdayPath] [chocoQuickCheck] [epnTodayPath] [epnYesterdayPath] [epnQuickCheck] [outputPath]"

# if no args specified, show usage
if [ $# -le 3 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

WORK_PATH=$1
CHOCO_TODAY_PATH=$2
CHOCO_YESTERDAY_PATH=$3
CHOCO_QUICK_CHECK=$4
EPN_TODAY_PATH=$5
EPN_YESTERDAY_PATH=$6
EPN_QUICK_CHECK=$7
OUTPUT_PATH=$8

DRIVER_MEMORY=10g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=16g
EXECUTOR_CORES=4

SPARK_EVENTLOG_DIR=hdfs://elvisha/app-logs/chocolate/logs/verifier
JOB_NAME="RuleVerifier0"

${SPARK_HOME}/bin/spark-submit \
    --class com.ebay.traffic.chocolate.sparknrt.verifier0.RuleVerifier0 \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${SPARK_JOB_CONF} \
    --conf spark.yarn.executor.memoryOverhead=8192 \
    --conf spark.eventLog.dir=${SPARK_EVENTLOG_DIR} \
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --workPath ${WORK_PATH} \
      --chocoTodayPath ${CHOCO_TODAY_PATH} \
      --chocoYesterdayPath ${CHOCO_YESTERDAY_PATH} \
      --chocoQuickCheck ${CHOCO_QUICK_CHECK} \
      --epnTodayPath ${EPN_TODAY_PATH} \
      --epnYesterdayPath ${EPN_YESTERDAY_PATH} \
      --epnQuickCheck ${EPN_QUICK_CHECK} \
      --outputPath ${OUTPUT_PATH}
