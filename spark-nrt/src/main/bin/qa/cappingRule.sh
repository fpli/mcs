#!/bin/bash
# run spark job on YARN - CappingRule

usage="Usage: cappingRule.sh [channel] [workDir] [outputDir] [archiveDir] [elasticsearchUrl] [partitions]"

# if no args specified, show usage
if [ $# -le 3 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env-qa.sh

CHANNEL=$1
WORK_DIR=$2
OUTPUT_DIR=$3
ARCHIVE_DIR=$4
ES_URL=$5
PARTITIONS=$6

DRIVER_MEMORY=1g
EXECUTOR_NUMBER=3
EXECUTOR_MEMORY=1g
EXECUTOR_CORES=1

JOB_NAME="cappingRule"

for f in $(find $bin/../../conf/qa -name '*.*');
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
    --conf spark.yarn.executor.memoryOverhead=1024 \
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --channel ${CHANNEL} \
      --workDir "${WORK_DIR}" \
      --outputDir ${OUTPUT_DIR} \
      --archiveDir ${ARCHIVE_DIR} \
      --elasticsearchUrl ${ES_URL} \
      --partitions ${PARTITIONS}
