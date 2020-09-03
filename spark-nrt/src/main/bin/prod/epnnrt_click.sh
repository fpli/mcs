#!/bin/bash
# run spark job on YARN - EPN Nrt Job

usage="Usage: epnnrt_click.sh [workDir] [resourceDir] [filterTime]"

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
OUTPUT_DIR=hdfs://elvisha/apps/epn-nrt

DRIVER_MEMORY=15g
EXECUTOR_NUMBER=40
EXECUTOR_MEMORY=18g
EXECUTOR_CORES=5

JOB_NAME="Chocolate_EPN_NRT_CLICK"

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.epnnrt.EpnNrtClickJob \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${SPARK_JOB_CONF} \
    --conf spark.yarn.executor.memoryOverhead=2048 \
    --conf spark.speculation=true \
    --conf spark.speculation.quantile=0.5 \
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --workDir ${WORK_DIR} \
      --partitions 3 \
      --resourceDir ${RESOURCE_DIR} \
      --filterTime ${FILTER_TIME} \
      --outputDir ${OUTPUT_DIR}