#!/bin/bash
# run spark job on YARN - epnReporting

usage="Usage: nonEpnReporting.sh [workDir] [archiveDir] [channel] [filterAction]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

WORK_DIR=$1
ARCHIVE_DIR=$2
CHANNEL=$3
FILTER_ACTION=$4

ES_URL=http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200

DRIVER_MEMORY=4g
EXECUTOR_NUMBER=5
EXECUTOR_MEMORY=6g
EXECUTOR_CORES=1

SPARK_EVENTLOG_DIR=hdfs://slickha/app-logs/spark/logs
HISTORY_SERVER=http://slcchocolatepits-1242733.stratus.slc.ebay.com:18080/
JOB_NAME="EPNReporting"

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.reporting.NonEPNReportingJob \
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
      --filterAction ${FILTER_ACTION} \
      --workDir "${WORK_DIR}" \
      --archiveDir ${ARCHIVE_DIR} \
      --elasticsearchUrl ${ES_URL} \
      --batchSize 10 \
      --hdfsUri hdfs://elvisha
