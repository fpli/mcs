#!/bin/bash
# run spark job on YARN - DedupeAndSink

usage="Usage: dedupeAndSink_v2.sh [channel] [kafkaTopic] [workDir] [outputDir] [partitions] [couchbaseDedupe]"

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
PARTITIONS=$5
CB_DEDUPE=$6

DRIVER_MEMORY=4g
EXECUTOR_NUMBER=20
EXECUTOR_MEMORY=6g
EXECUTOR_CORES=1

JOB_NAME="DedupeAndSink"

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.sink.DedupeAndSink_v2 \
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
      --outputDir ${OUTPUT_DIR} \
      --partitions ${PARTITIONS} \
      --maxConsumeSize 60000 \
      --couchbaseDedupe ${CB_DEDUPE}