#!/bin/bash
# run spark job on YARN - IPCappingRuleJob

usage="Usage: ipCappingRuleJob.sh [table] [writeTable] [time] [timeRange] [threshold]"

# if no args specified, show usage
if [ $# -le 2 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/chocolate-env.sh

TABLE=$1
WRITETABLE=$2
TIME=$3
TIMERANGE=$4
THRESHOLD=$5

DRIVER_MEMORY=10g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=12g
EXECUTOR_CORES=3

JOB_NAME="IPCappingRule"

for f in $(find $bin/../conf -name '*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.cappingrules.ip.IPCappingRuleJob \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${SPARK_JOB_CONF} \
    --conf spark.yarn.executor.memoryOverhead=8192 \
    ${bin}/../lib/chocolate-capping-rules-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --table ${TABLE} \
      --writeTable ${WRITETABLE} \
      --time ${TIME} \
      --timeRange ${TIMERANGE} \
      --threshold ${THRESHOLD}
