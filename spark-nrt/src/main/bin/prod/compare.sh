#!/bin/bash
# run spark job on YARN - Reporting

usage="Usage: compare.sh [click_source] [click_dest] [impression_source] [impression_dest][click_outputPath] [impression_outputPath]"

# if no args specified, show usage
if [ $# -lt 6 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

CLICK_SOURCE=$1
CLICK_DEST=$2
IMPRESSION_SOURCE=$13
IMPRESSION_DEST=$4
CLICK_OUTPUT_PATH=$5
IMPRESSION_OUTPUT_PATH=$6
CLICK_RUN=$7
IMPRESSION_RUN=$8
WORK_DIR=$9

DRIVER_MEMORY=10g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=32g
EXECUTOR_CORES=4

SPARK_EVENTLOG_DIR=hdfs://elvisha/app-logs/chocolate/logs/compare
JOB_NAME="Compare"

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.compare.CompareJob \
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
      --click_source ${CLICK_SOURCE} \
      --click_dest ${CLICK_DEST} \
      --impression_source ${IMPRESSION_SOURCE} \
      --impression_dest ${IMPRESSION_DEST} \
      --click_outputPath ${CLICK_OUTPUT_PATH} \
      --impression_outputPath ${IMPRESSION_OUTPUT_PATH} \
      --click_run ${CLICK_RUN} \
      --impression_run ${IMPRESSION_RUN} \
      --work_dir ${WORK_DIR}