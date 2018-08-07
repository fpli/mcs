#!/bin/bash
# run spark job on YARN - Reporting

usage="Usage: ruleVerifier.sh [inputPath1] [inputPath2]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

INPUT_PATH1=$1
INPUT_PATH2=$2

DRIVER_MEMORY=10g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=16g
EXECUTOR_CORES=4

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
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --inputPath1 ${INPUT_PATH1} \
      --inputPath2 ${INPUT_PATH2}
