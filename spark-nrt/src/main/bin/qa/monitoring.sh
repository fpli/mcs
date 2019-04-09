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

. ${bin}/../chocolate-env-qa.sh

CHANNEL=$1
WORK_DIR=$2
ES_URL=$3

DRIVER_MEMORY=1g
EXECUTOR_NUMBER=3
EXECUTOR_MEMORY=1g
EXECUTOR_CORES=1

JOB_NAME="Monitoring"

for f in $(find $bin/../../conf/qa -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.monitoring.MonitoringJob \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${SPARK_JOB_CONF} \
    --conf spark.yarn.executor.memoryOverhead=1024 \
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --channel ${CHANNEL} \
      --workDir "${WORK_DIR}" \
      --elasticsearchUrl ${ES_URL}