#!/bin/bash
# run spark job on YARN - EPN Nrt Job

usage="Usage: epnnrt.sh [workDir] [resourceDir] [filterTime]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

WORK_DIR=$1
RESOURCE_DIR=$2
FILTER_TIME=$3
OUTPUT_DIR=$4

DRIVER_MEMORY=6g
EXECUTOR_NUMBER=40
EXECUTOR_MEMORY=8g
EXECUTOR_CORES=5

JOB_NAME="EPN_Nrt"
SPARK_EVENTLOG_DIR=hdfs://elvisha/app-logs/chocolate/logs

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.epnnrt.EpnNrtJob \
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
      --workDir ${WORK_DIR} \
      --resourceDir ${RESOURCE_DIR} \
      --filterTime ${FILTER_TIME} \
      --outPutDir ${OUTPUT_DIR}