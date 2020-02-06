#!/bin/bash
# run spark job on YARN
# Dump and dedupe rover IMK data from crab result(crab transform jobs are running in LVS VMs) to IMK format.
# Input:    LVS HDFS
#           /apps/tracking-events-workdir/crabScp/dest
# Output:   SLC HDFS
#           /apps/tracking-events/crabDedupe
# Schedule: /5 * ? * *

usage="Usage: crabDedupe.sh [workDir] [inputDir] [outPutDir]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

WORK_DIR=$1
INTPUT_DIR=$2
OUTPUT_DIR=$3

ES_URL=http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200

DRIVER_MEMORY=4g
EXECUTOR_NUMBER=10
EXECUTOR_MEMORY=4g
EXECUTOR_CORES=2

JOB_NAME="crabDedupe"

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.crabDedupe.CrabDedupeJob \
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
      --workDir "${WORK_DIR}" \
      --inputDir "${INTPUT_DIR}" \
      --outputDir "${OUTPUT_DIR}" \
      --maxDataFiles 30 \
      --elasticsearchUrl ${ES_URL} \
      --couchbaseDedupe true \
      --partitions 3 \
      --snappyCompression true \
      --couchbaseDatasource appdlclickscbdbhost

