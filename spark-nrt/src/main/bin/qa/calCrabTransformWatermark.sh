#!/bin/bash
# run spark job on YARN
# imkCrabTransformOutputMerge.
# crabTransformDataDir:    SLC Imk CrabTransform Output Dir
#           hdfs://slickha/apps/tracking-events/crabTransform/imkOutput
# imkCrabTransformDataDir:   SLC Imk CrabTransform Merged Output Merged Dir
#           hdfs://slickha/apps/tracking-events/imkTransformMerged/imkOutput
# dedupAndSinkKafkaLagDir: LVS dedup and sink kafka lag dir
#           hdfs://elvisha/apps/tracking-events-workdir/last_ts
# channel: currently, only PAID_SEARCH
#           PAID_SEARCH
# outputDir: SLC watermark dir
#           hdfs://slickha/apps/tracking-events-workdir/calCrabTransformWatermark
# Schedule: /10 * ? * *

set -x

usage="Usage: calCrabTransformWatermark.sh [crabTransformDataDir] [imkCrabTransformDataDir] [dedupAndSinkKafkaLagDir] [channels] [outputDir]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env-qa.sh

CRAB_TRANSFORM_DATA_DIR=$1
IMK_CRABTRANSFORM_DATA_DIR=$2
DEDUP_AND_SINK_KAFKA_LAG_DIR=$3
CHANNELS=$4
OUTPUT_DIR=$5
ES_URL="http://10.148.181.34:9200"

DRIVER_MEMORY=1g
EXECUTOR_NUMBER=3
EXECUTOR_MEMORY=1g
EXECUTOR_CORES=1

JOB_NAME="calCrabTransformWatermark"

for f in $(find $bin/../../conf/qa -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.crabTransformWatermark.CalCrabTransformWatermark \
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
      --elasticsearchUrl ${ES_URL} \
      --crabTransformDataDir "${CRAB_TRANSFORM_DATA_DIR}" \
      --imkCrabTransformDataDir "${IMK_CRABTRANSFORM_DATA_DIR}" \
      --dedupAndSinkKafkaLagDir "${DEDUP_AND_SINK_KAFKA_LAG_DIR}" \
      --channels "${CHANNELS}" \
      --outputDir "${OUTPUT_DIR}"