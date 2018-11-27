#!/bin/bash
# run spark job on YARN - Reporting

usage="Usage: ruleVerifier.sh [workPath] [srcPath] [targetPath] [outputPath]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

WORK_PATH=$1
SRC_PATH=$2
TARGET_PATH=$3
OUTPUT_PATH=$4

DRIVER_MEMORY=10g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=16g
EXECUTOR_CORES=4

SPARK_EVENTLOG_DIR=hdfs://elvisha/app-logs/chocolate/logs/verifier
JOB_NAME="RuleVerifier"

${SPARK_HOME}/bin/spark-submit \
    --class com.ebay.traffic.chocolate.sparknrt.verifier.RuleVerifier \
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
      --srcPath ${SRC_PATH} \
      --targetPath ${TARGET_PATH} \
      --outputPath ${OUTPUT_PATH}
