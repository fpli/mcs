# run spark job on YARN - HDFSFileGeneratorJob

usage="Usage: hdfsFileGenerator.sh [originalTable] [channelType] [scanStopTime] [scanTimeWindow] [filePath] [numberOfPartition]"

# if no args specified, show usage
if [ $# -le 2 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd ..>/dev/null; pwd`

. ${bin}/chocolate-env.sh

ORIGINAL_TABLE=$1
CHANNEL_TYPE=$2
SCAN_STOP_TIME=$3
SCAN_TIME_WINDOW=$4
FILE_PATH=$5
NUMBER_OF_PARTITION=$6

DRIVER_MEMORY=10g
EXECUTOR_NUMBER=30
EXECUTOR_MEMORY=12g
EXECUTOR_CORES=3

JOB_NAME="HDFSFileGenerator"

for f in $(find $bin/../conf -name '*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.cappingrules.hdfs.HDFSFileGenerator \
    --name ${JOB_NAME} \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    ${SPARK_JOB_CONF} \
    --conf spark.yarn.executor.memoryOverhead=8192 \
    ${bin}/../lib/chocolate-capping-rules-*.jar \
      --jobName ${JOB_NAME} \
      --mode yarn \
      --originalTable ${ORIGINAL_TABLE} \
      --channelType ${CHANNEL_TYPE} \
      --scanStopTime "${SCAN_STOP_TIME}" \
      --scanTimeWindow ${SCAN_TIME_WINDOW} \
      --filePath "${FILE_PATH}" \
      --numberOfPartition ${NUMBER_OF_PARTITION} \
