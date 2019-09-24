#!/bin/bash
# run spark job on YARN
# imkCrabTransformOutputMerge.
# Input:    SLC Imk CrabTransform Output Dir
#           /apps/tracking-events/imkTransform
# Output:   SLC Imk CrabTransform Merged Output Merged Dir
#           /apps/tracking-events/imkTransformMerged
#           SLC Imk CrabTransform Backup Output Dir
#           /apps/tracking-events/imkTransformBackup
# Schedule: /10 * ? * *

set -x

usage="Usage: imkCrabTransformOutputMerge.sh [inputDir] [outputDir] [backupDir]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

INPUT_DIR=$1
OUTPUT_DIR=$2
BACKUP_DIR=$3
ES_URL=http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200

DRIVER_MEMORY=8g
EXECUTOR_NUMBER=60
EXECUTOR_MEMORY=4g
EXECUTOR_CORES=4

JOB_NAME="imkCrabTransformOutputMerge"

SPARK_EVENTLOG_DIR=hdfs://slickha/app-logs/chocolate/logs
HISTORY_SERVER=http://slcchocolatepits-1242733.stratus.slc.ebay.com:18080/

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.imkCrabTransformOutputMerge.ImkCrabTransformOutputMergeJob \
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
      --transformedPrefix chocolate_ \
      --elasticsearchUrl ${ES_URL} \
      --compressOutPut true \
      --inputDir "${INPUT_DIR}" \
      --outputDir "${OUTPUT_DIR}" \
      --backupDir "${BACKUP_DIR}"