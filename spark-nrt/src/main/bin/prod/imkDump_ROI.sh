#!/bin/bash
# run spark job on YARN - imkDump
# Dump long term IMK data from capping result to IMK format. The format is the same as Crab dedupe job's output.
# Input:    LVS HDFS
#           /apps/tracking-events-workdir
#           /apps/tracking-events
# Output:   LVS HDFS
#           /apps/tracking-events/channel/imkDump
# Schedule: /5 * ? * *

usage="Usage: imkDump.sh [channel] [workDir] [outPutDir] [elasticsearchUrl]"

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
OUTPUT_DIR=$3
ES_URL=$4

DRIVER_MEMORY=4g
EXECUTOR_NUMBER=5
EXECUTOR_MEMORY=4g
EXECUTOR_CORES=2

JOB_NAME="imkDump"

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.imkDump.ImkDumpRoiJob \
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
      --outPutDir "${OUTPUT_DIR}" \
      --partitions 1 \
      --elasticsearchUrl ${ES_URL}
