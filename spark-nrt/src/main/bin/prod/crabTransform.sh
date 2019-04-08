#!/bin/bash
# run spark job on YARN - crabsink

usage="Usage: crabSinkKafka.sh [workDir] [outPutDir]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

WORK_DIR=$1
OUTPUT_DIR=$2
ES_URL=http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200

DRIVER_MEMORY=10g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=12g
EXECUTOR_CORES=4

JOB_NAME="crabTransform"

SPARK_EVENTLOG_DIR=hdfs://slickha/app-logs/chocolate
HISTORY_SERVER=http://slcchocolatepits-1242733.stratus.slc.ebay.com:18080/

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done
#FILES=${FILES},file:///apache/hive/conf/hive-site.xml

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.crabTransform.CrabTransformJob \
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
      --channel crabSink \
      --transformedPrefix chocolate_ \
      --workDir "${WORK_DIR}" \
      --kwDataDir hdfs://slickha/apps/kw_lkp/2019-03-04/ \
      --outputDir "${OUTPUT_DIR}" \
      --compressOutPut true \
      --maxMetaFiles 20 \
      --elasticsearchUrl ${ES_URL} \
      --metaFile dedupe \
      --hdfsUri hdfs://slickha \
      --xidParallelNum 40

