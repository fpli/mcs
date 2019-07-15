#!/bin/bash
# run spark job on YARN - CappingRule
# Recover mode adjusts the partitions to 30 corresponding to 1 day delay. Adjust it by situation.

usage="Usage: cappingRule.sh [channel] [workDir] [outputDir] [archiveDir] [partitions] [elasticsearchUrl]"

# if no args specified, show usage
if [ $# -le 3 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

CHANNEL=$1
WORK_DIR=$2
OUTPUT_DIR=$3
ARCHIVE_DIR=$4
ES_URL=$5

DRIVER_MEMORY=4g
EXECUTOR_NUMBER=20
EXECUTOR_MEMORY=40g
EXECUTOR_CORES=1

SPARK_EVENTLOG_DIR=hdfs://elvisha/app-logs/chocolate/logs

JOB_NAME="cappingRule"

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.capping.CappingRuleJob \
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
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --channel ${CHANNEL} \
      --workDir "${WORK_DIR}" \
      --outputDir ${OUTPUT_DIR} \
      --archiveDir ${ARCHIVE_DIR} \
      --partitions 30 \
      --elasticsearchUrl ${ES_URL}
