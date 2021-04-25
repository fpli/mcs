#!/bin/bash
# run spark job on YARN - EPN Nrt Job

usage="Usage: epnnrt_click_v2.sh [inputWorkDir] [outputWorkDir] [resourceDir] [filterTime] [outputDir]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

INPUT_WORK_DIR=$1
OUTPUT_WORK_DIR=$2
RESOURCE_DIR=$3
FILTER_TIME=$4
OUTPUT_DIR=$5

DRIVER_MEMORY=1g
EXECUTOR_NUMBER=3
EXECUTOR_MEMORY=1g
EXECUTOR_CORES=1

JOB_NAME="Chocolate_EPN_NRT_CLICK_v2"

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.epnnrt_v2.EpnNrtClickJob_v2 \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${SPARK_JOB_CONF} \
    --conf spark.yarn.executor.memoryOverhead=1024 \
    --conf spark.speculation=true \
    --conf spark.speculation.quantile=0.5 \
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --inputWorkDir ${INPUT_WORK_DIR} \
      --outputWorkDir ${OUTPUT_WORK_DIR} \
      --partitions 3 \
      --resourceDir ${RESOURCE_DIR} \
      --filterTime ${FILTER_TIME} \
      --outputDir ${OUTPUT_DIR}