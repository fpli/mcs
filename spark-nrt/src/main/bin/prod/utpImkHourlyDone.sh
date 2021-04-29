#!/bin/bash
set -x

CLUSTER=${1}

if [ "${CLUSTER}" == "rno" ]
then
    HDFS_PATH="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs"
    HIVE_PATH="/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive"
    SPARK_PATH="/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit"
    QUEUE="hdlq-commrce-mkt-tracking-high-mem"
    INPUT_SOURCE="choco_data.imk_rvr_trckng_event"
    CACHE_TABLE="choco_data.utp_imk_hourly_done_cache"
    CACHE_DIR="viewfs://apollo-rno/apps/b_marketing_tracking/work/UTPImkHourlyDoneJob/cache"
    DONE_FILE_DIR="viewfs://apollo-rno/apps/b_marketing_tracking/watch"
    JOB_DIR="viewfs://apollo-rno/apps/b_marketing_tracking/work/UTPImkHourlyDoneJob"
elif [ "${CLUSTER}" == "hercules" ]
then
    HDFS_PATH="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs"
    HIVE_PATH="/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive"
    SPARK_PATH="/datashare/mkttracking/tools/hercules_lvs/spark-hercules/bin/spark-submit"
    QUEUE="hdlq-data-default"
    INPUT_SOURCE="im_tracking.imk_rvr_trckng_event_v2"
    CACHE_TABLE="im_tracking.utp_imk_hourly_done_cache"
    CACHE_DIR="/apps/b_marketing_tracking/work/UTPImkHourlyDoneJob/cache"
    DONE_FILE_DIR="/apps/b_marketing_tracking/watch"
    JOB_DIR="/apps/b_marketing_tracking/work/UTPImkHourlyDoneJob"
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

first_done_file=${DONE_FILE_DIR}/${done_dt}/imk_rvr_trckng_event_hourly.done.${done_dt}0000000000

${HDFS_PATH} dfs -test -e ${first_done_file}
first_done_file_exists=$?
if [ ${first_done_file_exists} -eq 1 ]; then
    echo "${done_dt} first done file not exist: ${first_done_file}"
    echo "======================== Add partition to Hive ========================"
    retry_add=1
    rcode_add=1
    until [[ ${retry_add} -gt 3 ]]
    do
        ${HIVE_PATH} -e "set hive.msck.path.validation=ignore; ALTER TABLE ${INPUT_SOURCE} ADD IF NOT EXISTS PARTITION (dt='${partition_dt}')"
        rcode_add=$?
        if [ ${rcode_add} -eq 0 ]
        then
            break
        else
            echo "Failed to add ${partition_dt} partition to hive."
            retry_add=$(expr ${retry_add} + 1)
        fi
    done
    if [ ${rcode_add} -ne 0 ]
    then
        echo -e "Failed to add ${partition_dt} partition on hive, please check!!!"
        exit ${rcode_add}
    fi
fi

for f in $(find $bin/../../conf/prod -name '*.*'); do
  FILES=${FILES},file://$f;
done

${SPARK_PATH} \
--files ${FILES} \
--class com.ebay.traffic.chocolate.sparknrt.hourlyDone.UTPImkHourlyDoneJob \
--master yarn \
--deploy-mode cluster \
--queue ${QUEUE} \
--num-executors 40 \
--executor-memory 8G \
--executor-cores 4 \
${bin}/../../lib/chocolate-spark-nrt-*.jar \
--inputSource ${INPUT_SOURCE} \
--cacheTable ${CACHE_TABLE} \
--cacheDir ${CACHE_DIR} \
--doneFileDir ${DONE_FILE_DIR} \
--jobDir ${JOB_DIR} \
--doneFilePrefix imk_rvr_trckng_event_hourly.done.