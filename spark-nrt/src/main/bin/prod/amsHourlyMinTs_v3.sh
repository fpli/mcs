#!/bin/bash
# run spark job on YARN - Check ams data Minimum Timestamp

usage="Usage: checkAmsMinTs_v2.sh [workDir] [channel] [usage] [metaSuffix] [outputDir]"

# if no args specified, show usage
if [ $# -le 4 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`


WORK_DIR=$1
CHANNEL=$2
USAGE=$3
META_SUFFIX=$4
OUTPUT_DIR=$5

DRIVER_MEMORY=4g
EXECUTOR_NUMBER=5
EXECUTOR_MEMORY=4g
EXECUTOR_CORES=1

JOB_NAME="AMSHourlyMinTsJob_v3"
/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit \
    --class com.ebay.traffic.chocolate.sparknrt.amsHourlyMinTsV2.AmsHourlyMinTsJobV2 \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --queue hdlq-commrce-mkt-tracking-high-mem\
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    --conf spark.yarn.executor.memoryOverhead=8192 \
    ${bin}/../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --workDir "${WORK_DIR}" \
      --channel ${CHANNEL} \
      --usage ${USAGE} \
      --metaSuffix ${META_SUFFIX} \
      --outputDir ${OUTPUT_DIR}
