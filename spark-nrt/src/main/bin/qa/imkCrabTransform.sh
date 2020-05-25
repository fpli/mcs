#!/bin/bash
# run spark job on YARN
# Run the same job in UC4 job to join tables and generate final tables. Source is long term imk dump result.
# Input:    LVS Hadoop
#           /apps/tracking-events/channel/imkDump
# Output:   SLC Hadoop
#           /apps/tracking-events/imkTransform
# Schedule: * * ? * *

usage="Usage: imkCrabTransform.sh [channel] [workDir] [outPutDir]"

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
OUTPUT_DIR=$3
ES_URL=""

KW_LK_FOLDER=/apps/kw_lkp/2019-04-14/

DRIVER_MEMORY=1g
EXECUTOR_NUMBER=3
EXECUTOR_MEMORY=1g
EXECUTOR_CORES=1

JOB_NAME="imkCrabTransform"

for f in $(find $bin/../../conf/qa -name '*.*');
do
  FILES=${FILES},file://$f;
done

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
    --conf spark.yarn.executor.memoryOverhead=1024 \
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --channel "${CHANNEL}" \
      --transformedPrefix chocolate_ \
      --kwDataDir "${KW_LK_FOLDER}" \
      --workDir "${WORK_DIR}" \
      --outputDir "${OUTPUT_DIR}" \
      --compressOutPut true \
      --maxMetaFiles 100 \
      --elasticsearchUrl ${ES_URL} \
      --metaFile imkDump \
      --hdfsUri hdfs://choco-cent-1659401.slc07.dev.ebayc3.com:8020 \
      --xidParallelNum 20