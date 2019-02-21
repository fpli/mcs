#!/bin/bash
# run spark job on YARN - imkTransform

usage="Usage: imkTransform.sh [channel] [workDir] [outputDir]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

CHANNEL=$1
WORK_DIR=$2
OUTPUT_DIR=$3

DRIVER_MEMORY=2g
EXECUTOR_NUMBER=5
EXECUTOR_MEMORY=10g
EXECUTOR_CORES=1

SPARK_EVENTLOG_DIR=hdfs://elvisha/app-logs/chocolate/logs/imkTransform
JOB_NAME="imkTransform"

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.imkTransform.ImkTransformJob \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${SPARK_JOB_CONF} \
    --conf spark.yarn.executor.memoryOverhead=1024 \
    --conf spark.eventLog.dir=${SPARK_EVENTLOG_DIR} \
    ${bin}/../../lib/tfs-chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --channel ${CHANNEL} \
      --transformedPrefix chocolate_ \
      --workDir ${WORK_DIR} \
      --outputDir ${OUTPUT_DIR} \
      --compressOutPut true
