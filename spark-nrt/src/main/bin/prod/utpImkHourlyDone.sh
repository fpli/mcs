#!/bin/bash
set -x

CLUSTER=${1}

if [ "${CLUSTER}" == "rno" ]
then
    SPARK_PATH="/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit"
    QUEUE="hdlq-commrce-product-high-mem"
    INPUT_SOURCE="choco_data.utp_imk_trckng_event"
    CACHE_TABLE="choco_data.utp_imk_hourly_done_cache"
    CACHE_DIR="viewfs://apollo-rno/apps/b_marketing_tracking/work/UTPImkHourlyDoneJob/cache"
    DONE_FILE_DIR="viewfs://apollo-rno/apps/b_marketing_tracking/work/UTPImkHourlyDoneJob/watch"
    JOB_DIR="viewfs://apollo-rno/apps/b_marketing_tracking/work/UTPImkHourlyDoneJob"
elif [ "${CLUSTER}" == "hercules" ]
then
    SPARK_PATH="/datashare/mkttracking/tools/hercules_lvs/spark-hercules/bin/spark-submit"
    QUEUE="hdlq-data-default"
    INPUT_SOURCE="im_tracking.utp_imk_trckng_event"
    CACHE_TABLE="im_tracking.utp_imk_hourly_done_cache"
    CACHE_DIR="/apps/b_marketing_tracking/work/UTPImkHourlyDoneJob/cache"
    DONE_FILE_DIR="/apps/b_marketing_tracking/work/UTPImkHourlyDoneJob/watch"
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