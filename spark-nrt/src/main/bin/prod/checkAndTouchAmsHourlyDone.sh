#!/usr/bin/env bash

############################################### Hourly done conditions ###############################################
# ams must meet all condition to generate hourly done file
# 1. Dedupe lags are all in the new hour;
# 2. Message timestamp are all in the new hour;
######################################################################################################################

WORK_DIR=$1
CHANNEL=$2
USAGE=$3
META_SUFFIX=$4
LOCAL_DONE_DATE_FILE=$5
MIN_TS_FILE=$6
TYPE=$7

export HADOOP_USER_NAME=chocolate
apollo_command=/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs


function get_current_done(){
    last_done=`cat $1`
    last_ts=`date -d "${last_done:0:8} ${last_done:8}" +%s`
    let current_ts=${last_ts}+3600
    current_done=`date -d @${current_ts} "+%Y%m%d%H"`
    echo ${current_done}
}

################################################## Check dedupe lag ##################################################

echo "================================ Start checking dedupe lags ================================"

flag_lag=0
last_ts_path=viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir/last_ts/EPN/*

earliest_ts=`${apollo_command} dfs -cat ${last_ts_path} | sort -n | head -1`
echo "Timestamp of earliest dedupe lag: "${earliest_ts}


################################################# Check data timestamp #################################################

echo "============================== Start checking data timestamp =============================="

## Check if there is any meta to check
let meta_num=`${apollo_command} dfs -ls ${WORK_DIR}'/meta/'${CHANNEL}'/output/'${USAGE} | grep ${META_SUFFIX} | wc -l`

if [ ${meta_num} -gt 0 ]
then
    echo "There are ${meta_num} metas."
    flag_meta=1
    ./amsHourlyMinTs_v3.sh ${WORK_DIR} ${CHANNEL} ${USAGE} ${META_SUFFIX} ${MIN_TS_FILE}
    rcode_job=$?
    if [ ${rcode_job} -ne 0 ]
    then
        echo -e "Failed to check data minimum timestamp!!! It's just for dirty data warning, no need to take action." | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT WARNING] Error in checking data timestamp!!!" -v DL-eBay-Chocolate-GC@ebay.com
    fi
else
    echo "No meta! No need to check!"
fi

let data_min_ts=`${apollo_command} dfs -cat ${MIN_TS_FILE}`

################################################## Define Check Hour ##################################################
for ((a=1;a<5; a++))
do
    flag_ts=0
    flag_lag=0
    echo "============== Search for last hourly done date to define current check hour =============="

    local_done_date=`cat ${LOCAL_DONE_DATE_FILE}`
    check_last_time=${local_done_date:0:4}'-'${local_done_date:4:2}'-'${local_done_date:6:2}' '${local_done_date:8}':00:00'
    echo "Last done: "${check_last_time}
    check_last_timestamp=$(date -d "${check_last_time}" +%s)000
    let check_now_timestamp=${check_last_timestamp}+7200000


    echo "Timestamp of earliest epn nrt data: "${data_min_ts}
    if [ ${data_min_ts} -ge ${check_now_timestamp} ]
    then
        echo "Data timestamp are all in current hour"
        flag_ts=1
    fi
    if [ ${earliest_ts} -ge ${check_now_timestamp} ]
    then
         echo "Dedupe lags are all in current hour"
         flag_lag=1
    fi

    ##################################################### Final Check #####################################################
    if [[ ${flag_lag} -eq 1 && ${flag_ts} -eq 1 && ${flag_meta} -eq 1 ]]
    then
        echo "Hourly data is ready"
        current_done_click=$(get_current_done ${LOCAL_DONE_DATE_FILE})
        echo "=================== Start touching reno click hourly done file ==================="
        ./touchAmsHourlyDone_v4.sh ${current_done_click} ${LOCAL_DONE_DATE_FILE} ${TYPE} reno
        ./touchAmsHourlyDone_v4.sh ${current_done_click} ${LOCAL_DONE_DATE_FILE} ${TYPE} hercules
    else
        echo "Hourly data is not ready"
        exit 0
    fi
done
