#!/bin/bash
# run spark job on YARN - Check ams data Minimum Timestamp

usage="Usage: checkAmsMinTs.sh [workDir] [channel] [usage] [metaSuffix] [outputDir]"

# if no args specified, show usage
if [ $# -le 5 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

WORK_DIR=$1
CHANNEL=$2
USAGE=$3
META_SUFFIX=$4
OUTPUT_DIR=$5

DRIVER_MEMORY=4g
EXECUTOR_NUMBER=5
EXECUTOR_MEMORY=4g
EXECUTOR_CORES=1

SPARK_EVENTLOG_DIR=hdfs://elvisha/app-logs/chocolate/logs/

JOB_NAME="AMSHourlyMinTsJob"

${SPARK_HOME}/bin/spark-submit \
    --class com.ebay.traffic.chocolate.sparknrt.amsHourlyMinTs.AmsHourlyMinTsJob \
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
      --workDir "${WORK_DIR}" \
      --channel ${CHANNEL} \
      --usage ${USAGE} \
      --metaSuffix ${META_SUFFIX} \
      --outputDir ${OUTPUT_DIR}
