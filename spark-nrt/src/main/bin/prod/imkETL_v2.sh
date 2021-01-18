#!/bin/bash
# run spark job on YARN
# Run the same job in UC4 job to join tables and generate final tables. Source is legacy imk dedupe result.
# Input:    Dedupe or capping output
#           /apps/tracking-events
# Output:   output dir
#           /apps/tracking-events
# Schedule: * * ? * *

usage="Usage: imkETL_v2.sh [channel] [workDir] [outPutDir]"

set -x

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=$(dirname "$0")
bin=$(
  cd "$bin" >/dev/null
  pwd
)

. ${bin}/../chocolate-env.sh

CHANNEL=$1
WORK_DIR=$2
OUTPUT_DIR=$3
ES_URL=http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200

DRIVER_MEMORY=8g
EXECUTOR_NUMBER=50
EXECUTOR_MEMORY=8g
EXECUTOR_CORES=2

JOB_NAME="IMK_ETL_V2"

for f in $(find $bin/../../conf/prod-imk -name '*.*'); do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
  --files ${FILES} \
  --class com.ebay.traffic.chocolate.sparknrt.imkETLV2.ImkETLV2Job \
  --name ${JOB_NAME} \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory ${DRIVER_MEMORY} \
  --num-executors ${EXECUTOR_NUMBER} \
  --executor-memory ${EXECUTOR_MEMORY} \
  --executor-cores ${EXECUTOR_CORES} \
  ${SPARK_JOB_CONF} \
  --conf spark.yarn.executor.memoryOverhead=8192 \
  ${bin}/../../lib_imk/chocolate-spark-nrt-3.7.0-RELEASE-fat.jar \
  --appName ${JOB_NAME} \
  --mode yarn \
  --channel ${CHANNEL} \
  --transformedPrefix chocolate_ \
  --workDir "${WORK_DIR}" \
  --outPutDir "${OUTPUT_DIR}" \
  --outputFormat "parquet" \
  --compressOutPut "true" \
  --elasticsearchUrl ${ES_URL} \
  --xidParallelNum 60