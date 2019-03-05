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

DRIVER_MEMORY=8g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=8g
EXECUTOR_CORES=4

SPARK_EVENTLOG_DIR=hdfs://slickha/app-logs/chocolate/logs/monitoring
HISTORY_SERVER=http://slcchocolatepits-1242733.stratus.slc.ebay.com:18080/

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
    --conf spark.eventLog.dir=${SPARK_EVENTLOG_DIR} \
    --conf spark.yarn.historyServer.address=${HISTORY_SERVER} \
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --channel ${CHANNEL} \
      --workDir "${WORK_DIR}" \
      --elasticsearchUrl ${ES_URL}
