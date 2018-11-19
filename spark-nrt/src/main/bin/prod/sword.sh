#!/bin/bash
# run spark job on YARN - Sword

usage="Usage: sword.sh [channel] [dataDir] [workDir]"

# if no args specified, show usage
if [ $# -le 2 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

CHANNEL=$1
DATA_DIR=$2
WORK_DIR=$3

DRIVER_MEMORY=1g
EXECUTOR_NUMBER=3
EXECUTOR_MEMORY=1g
EXECUTOR_CORES=1
BOOTSTRAP_SERVERS=chocolate-kafka.vip.lvs.ebay.com:6667
KAFKA_TOPIC=zelda-sword-preprod

SPARK_EVENTLOG_DIR=hdfs://elvisha/app-logs/chocolate/logs/sword
JOB_NAME="sword"

${SPARK_HOME}/bin/spark-submit \
    --class com.ebay.traffic.chocolate.sparknrt.sword.SwordJob \
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
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --channel ${CHANNEL} \
      --dataDir "${DATA_DIR}" \
      --workDir ${WORK_DIR} \
      --bootstrapServers ${BOOTSTRAP_SERVERS} \
      --kafkaTopic ${KAFKA_TOPIC}