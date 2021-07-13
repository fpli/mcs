#!/bin/bash
set -x

CLUSTER=${1}

if [ "${CLUSTER}" == "rno" ]
then
    HDFS_PATH="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs"
    HIVE_PATH="/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive"
    SPARK_PATH="/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit"
    QUEUE="hdlq-commrce-mkt-tracking-high-mem"
    INPUT_SOURCE="choco_data.utp_event"
    CACHE_TABLE="choco_data.utp_hourly_done_cache"
    CACHE_DIR="viewfs://apollo-rno/apps/b_marketing_tracking/work/UTPHourlyDoneJob/cache"
    DONE_FILE_DIR="viewfs://apollo-rno/apps/b_marketing_tracking/watch"
    JOB_DIR="viewfs://apollo-rno/apps/b_marketing_tracking/work/UTPHourlyDoneJob"
elif [ "${CLUSTER}" == "hercules" ]
then
    HDFS_PATH="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs"
    HIVE_PATH="/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive"
    SPARK_PATH="/datashare/mkttracking/tools/hercules_lvs/spark-hercules/bin/spark-submit"
    QUEUE="hdlq-data-batch-low"
    INPUT_SOURCE="im_tracking.utp_event"
    CACHE_TABLE="im_tracking.utp_hourly_done_cache"
    CACHE_DIR="/apps/b_marketing_tracking/work/UTPHourlyDoneJob/cache"
    DONE_FILE_DIR="/apps/b_marketing_tracking/watch"
    JOB_DIR="/apps/b_marketing_tracking/work/UTPHourlyDoneJob"
else
    echo "Wrong cluster to send data!"
    exit 1
fi

bin=$(dirname "$0")
bin=$(
  cd "$bin" >/dev/null
  pwd
)

if [[ $? -ne 0 ]]; then
  echo "get latest path failed"
  exit 1
fi

current_date=$(date)
done_dt=$(date -d "${current_date}" '+%Y%m%d')
partition_dt=$(date -d "${current_date}" '+%Y-%m-%d')

for f in $(find $bin/../../conf/prod -name '*.*'); do
  FILES=${FILES},file://$f;
done

${SPARK_PATH} \
--files ${FILES} \
--class com.ebay.traffic.chocolate.sparknrt.hourlyDone.UTPHourlyDoneJob \
--master yarn \
--deploy-mode cluster \
--queue ${QUEUE} \
--num-executors 40 \
--executor-memory 8G \
--executor-cores 4 \
${bin}/../../lib/chocolate-spark-nrt-*.jar \
--appName UTPHourlyDoneJob \
--inputSource ${INPUT_SOURCE} \
--cacheTable ${CACHE_TABLE} \
--cacheDir ${CACHE_DIR} \
--doneFileDir ${DONE_FILE_DIR} \
--jobDir ${JOB_DIR} \
--doneFilePrefix utp_event_hourly.done.