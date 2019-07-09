#!/usr/bin/env bash

############################################ Hourly done conditions ############################################
# ams must meet all condition to generate hourly done file
# 1. Dedupe lags are all in the new hour;
# 2. Message timestamp are all in the new hour;
################################################################################################################

WORK_DIR=$1
CHANNEL=$2
USAGE=$3
META_SUFFIX=$4
LOCAL_DONE_DATE_FILE=$5

export HADOOP_USER_NAME=chocolate

############################################### Define Check Hour ###############################################

echo "====================== Search for last hourly done date to define current check hour ======================"

local_done_date=`cat ${LOCAL_DONE_DATE_FILE}`
check_last_time=${local_done_date:0:4}'-'${local_done_date:4:2}'-'${local_done_date:6:2}' '${local_done_date:8}':00:00'
echo "Last done: "${check_last_time}
check_last_timestamp=$(date -d "${check_last_time}" +%s)000
let check_now_timestamp=${check_last_timestamp}+7200000


############################################### Check dedupe lag ###############################################

echo "====================== Start checking dedupe lags ======================"

flag_lag=0
last_ts_path=/apps/tracking-events-workdir/last_ts/EPN/*

earliest_ts=`hdfs dfs -cat ${last_ts_path} | sort -n | head -1`
echo "Timestamp of earliest dedupe lag: "${earliest_ts}
if [ ${earliest_ts} -ge ${check_now_timestamp} ]
then
     echo "Dedupe lags are all in current hour"
     flag_lag=1
fi


############################################## Check data timestamp ##############################################

echo "====================== Start checking data timestamp ======================"

flag_ts=0
data_min_ts_file=/apps/epn-nrt/min_ts.txt

./amsHourlyMinTs.sh ${WORK_DIR} ${CHANNEL} ${USAGE} ${META_SUFFIX} ${data_min_ts_file}
let data_min_ts=`hdfs dfs -cat ${data_min_ts_file}`
echo "Timestamp of earliest epn nrt data: "${data_min_ts}
if [ ${data_min_ts} -ge ${check_now_timestamp} ]
then
    echo "Data timestamp are all in current hour"
    flag_ts=1
fi


################################################### Final Check ###################################################
if [[ ${flag_lag} -eq 1 && ${flag_ts} -eq 1 ]]
then
    echo "Hourly data is ready"
    exit 1
else
    echo "Hourly data is not ready"
    exit 0
fi