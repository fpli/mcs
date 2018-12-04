#!/bin/bash
# run spark job on YARN - Sword

usage="Usage: sword.sh [channel] [dataDir] [workDir]"

# if no args specified, show usage
if [ $# -le 2 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env-qa.sh

CHANNEL=$1
DATA_DIR=$2
WORK_DIR=$3

DRIVER_MEMORY=1g
EXECUTOR_NUMBER=3
EXECUTOR_MEMORY=1g
EXECUTOR_CORES=1
BOOTSTRAP_SERVERS=choco-2217601.lvs02.dev.ebayc3.com:6667,choco-cent-1659401.slc07.dev.ebayc3.com:6667,choco-cent-2218337.lvs02.dev.ebayc3.com:6667
KAFKA_TOPIC=zelda-sword

JOB_NAME="sword"

for f in $(find $bin/../../conf/qa -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.sword.SwordJob \
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
      --channel ${CHANNEL} \
      --dataDir "${DATA_DIR}" \
      --workDir ${WORK_DIR} \
      --bootstrapServers ${BOOTSTRAP_SERVERS} \
      --kafkaTopic ${KAFKA_TOPIC}
