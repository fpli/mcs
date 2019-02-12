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

DRIVER_MEMORY=1g
EXECUTOR_NUMBER=3
EXECUTOR_MEMORY=1g
EXECUTOR_CORES=1


SPARK_EVENTLOG_DIR=hdfs://slickha/app-logs/crabTransform
HISTORY_SERVER=http://slcchocolatepits-1242733.stratus.slc.ebay.com:18080/
JOB_NAME="imkTransform"

for f in $(find $bin/../../conf/qa -name '*.*');
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
    --conf spark.yarn.historyServer.address=${HISTORY_SERVER} \
    ${bin}/../../lib/tfs-chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --channel ${CHANNEL} \
      --transformedPrefix chocolate- \
      --workDir ${WORK_DIR} \
      --outputDir ${OUTPUT_DIR} \
      --compressOutPut true
