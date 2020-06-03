#!/bin/bash
# run spark job on YARN - Monitoring

usage="Usage: monitoring.sh [channel] [workDir] [elasticsearchUrl]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

CHANNEL=$1
WORK_DIR=$2
ES_URL=$3

DRIVER_MEMORY=4g
EXECUTOR_NUMBER=20
EXECUTOR_MEMORY=4g
EXECUTOR_CORES=1

JOB_NAME="Monitoring"

${SPARK_HOME}/bin/spark-submit \
    --class com.ebay.traffic.chocolate.sparknrt.monitoring.MonitoringJob \
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
      --workDir "${WORK_DIR}" \
      --elasticsearchUrl ${ES_URL}
