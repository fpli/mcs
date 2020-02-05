#!/bin/bash
# run spark job on YARN
# Touch imk hourly done.
# Input:    SLC Watermark HDFS
#           /apps/tracking-events-workdir/calCrabTransformWatermark
# Output:   SLC Done HDFS
#           /apps/tracking-events/watch
# Schedule: /3 * ? * *

set -x

usage="Usage: touchImkHourlyDone.sh [workDir] [lagDir] [doneDir]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

WORK_DIR=$1
LAG_DIR=$2
DONE_DIR=$3

dt_hour=$(date -d '1 hour ago' +%Y%m%d%H)
dt=${dt_hour:0:8}

done_file=${DONE_DIR}/${dt}/imk_rvr_trckng_event_hourly.done.${dt_hour}00000000
hdfs dfs -test -e ${done_file}
done_file_exists=$?
if [ ${done_file_exists} -eq 0 ]; then
    echo "done file exists: ${done_file}"
    exit 0
fi

DRIVER_MEMORY=4g
EXECUTOR_NUMBER=5
EXECUTOR_MEMORY=4g
EXECUTOR_CORES=2

JOB_NAME="touchImkHourlyDone"

SPARK_EVENTLOG_DIR=hdfs://slickha/spark-history-logs/chocolate/logs
HISTORY_SERVER=http://slcchocolatepits-1242733.stratus.slc.ebay.com:18080/

for f in $(find $bin/../../conf/prod -name '*.*');
do
  FILES=${FILES},file://$f;
done

${SPARK_HOME}/bin/spark-submit \
    --files ${FILES} \
    --class com.ebay.traffic.chocolate.sparknrt.hercules.TouchImkHourlyDoneJob \
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
      --workDir "${WORK_DIR}" \
      --lagDir "${LAG_DIR}" \
      --doneDir "${DONE_DIR}"