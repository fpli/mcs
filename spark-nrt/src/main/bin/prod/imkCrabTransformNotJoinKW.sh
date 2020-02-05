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

. ${bin}/../chocolate-env.sh

CHANNEL=$1
WORK_DIR=$2
OUTPUT_DIR=$3
JOIN_KEYWORD=$4
ES_URL=http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200

KW_LKP_LATEST_PATH=hdfs://slickha/apps/kw_lkp/latest_path

KW_LKP_FOLDER=$(hdfs dfs -text ${KW_LKP_LATEST_PATH})

if [[ $? -ne 0 ]]; then
   echo "get latest path failed"
   exit 1
fi

DRIVER_MEMORY=8g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=2g
EXECUTOR_CORES=4

JOB_NAME="imkCrabTransform"

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
    --conf spark.yarn.executor.memoryOverhead=1024 \
    ${bin}/../../lib/chocolate-spark-nrt-*.jar \
      --appName ${JOB_NAME} \
      --mode yarn \
      --channel "${CHANNEL}" \
      --transformedPrefix chocolate_ \
      --kwDataDir "${KW_LKP_FOLDER}" \
      --workDir "${WORK_DIR}" \
      --outputDir "${OUTPUT_DIR}" \
      --joinKeyword "${JOIN_KEYWORD}" \
      --compressOutPut true \
      --maxMetaFiles 100 \
      --elasticsearchUrl ${ES_URL} \
      --metaFile imkDump \
      --hdfsUri hdfs://elvisha \
      --xidParallelNum 20