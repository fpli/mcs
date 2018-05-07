#!/bin/bash
# run spark job on YARN - DedupeAndSink

usage="Usage: dedupeAndSink.sh [channel] [kafkaTopic] [workDir] [outputDir]"

# if no args specified, show usage
if [ $# -le 3 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

CHANNEL=$1
KAFKA_TOPIC=$2
WORK_DIR=$3
OUTPUT_DIR=$4

DRIVER_MEMORY=10g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=16g
EXECUTOR_CORES=4

JOB_NAME="DedupeAndSink"

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.sink.DedupeAndSink \
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
      --channel ${CHANNEL} \
      --kafkaTopic ${KAFKA_TOPIC} \
      --workDir "${WORK_DIR}" \
      --outputDir ${OUTPUT_DIR}
