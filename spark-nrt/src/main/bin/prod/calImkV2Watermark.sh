#!/bin/bash
# run spark job on YARN
# imkCrabTransformDataDir:   SLC Imk CrabTransform Merged Output Merged Dir
#           hdfs://slickha/apps/tracking-events/imkTransform/imkOutput
# dedupAndSinkKafkaLagDir: LVS dedup and sink kafka lag dir
#           hdfs://elvisha/apps/tracking-events-workdir-imk/last_ts
# channel: currently, only PAID_SEARCH
#           PAID_SEARCH
# outputDir: SLC watermark dir
#           hdfs://slickha/apps/tracking-events-workdir-imk/calCrabTransformWatermark
# Schedule: /10 * ? * *

set -x

usage="Usage: calImkV2Watermark.sh [imkCrabTransformDataDir] [dedupAndSinkKafkaLagDir] [channels] [outputDir]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

IMK_CRABTRANSFORM_DATA_DIR=$1
DEDUP_AND_SINK_KAFKA_LAG_DIR=$2
CHANNELS=$3
OUTPUT_DIR=$4
ES_URL="http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200"

DRIVER_MEMORY=1g
EXECUTOR_NUMBER=3
EXECUTOR_MEMORY=1g
EXECUTOR_CORES=1

JOB_NAME="calImkV2Watermark"

for f in $(find $bin/../../conf/prod-imk -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.calImkV2Watermark.CalImkV2Watermark \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${SPARK_JOB_CONF} \
    --conf spark.yarn.executor.memoryOverhead=1024 \
    ${bin}/../../lib_imk/chocolate-spark-nrt-3.7.0-RELEASE-fat.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --elasticsearchUrl ${ES_URL} \
      --imkCrabTransformDataDir "${IMK_CRABTRANSFORM_DATA_DIR}" \
      --dedupAndSinkKafkaLagDir "${DEDUP_AND_SINK_KAFKA_LAG_DIR}" \
      --channels "${CHANNELS}" \
      --outputDir "${OUTPUT_DIR}"