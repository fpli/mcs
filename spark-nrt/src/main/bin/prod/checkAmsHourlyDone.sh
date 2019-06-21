#!/usr/bin/env bash

############################################ Hourly done conditions ############################################
# ams must meet all condition to generate hourly done file
# 1. Dedupe lags are all in the new hour;
# 2. Message timestamp are all in the new hour;
# 3. Data is sent to Heculers successfully.
################################################################################################################

RENO_DATA_FILE=$1

############################################### Define Check Hour ###############################################
echo "Search for last hourly done date to define current check hour."
local_done_date="/datashare/mkttracking/chocolate/epn-nrt/local_done_date.txt"
check_last_time=${local_done_date:0:4}'-'${local_done_date:4:2}'-'${local_done_date:6:2}' '${local_done_date:8:2}':00:00'
check_last_timestamp=$(date -d ${check_time}" +%s)000
let check_now_timestamp=${check_last_timestamp}+7200000


############################################### Check dedupe lag ###############################################
echo "Start checking dedupe lags."
flag_lag=0
LAST_TS_PATH=/apps/tracking-events-workdir/last_ts/EPN/*
earliest_ts=`hdfs dfs -cat ${LAST_TS_PATH} | sort -n | head -1`
echo "Timestamp of earliest message: "${last_ts}
if [ ${earliest_ts} -ge ${check_now_timestamp} ]
then
     echo "Dedupe lags are all in current hour"
     flag_lag=1
fi


############################################## Check data timestamp ##############################################
echo "Start checking data timestamp."
flag_ts=0
data_ts_dir="/datashare/mkttracking/chocolate/epn-nrt/flag_ts.txt"
./checkEpnNrtTs.sh ${RENO_DATA_FILE} ${data_ts_dir}
data_ts=cat ${data_ts_dir}
if [ ${data_ts} -ge ${check_now_timestamp} ]
then
    echo "Data timestamp are all in current hour"
    flag_ts=1
fi


################################################### Final Check ###################################################
if [[ ${flag1} -eq 1 && ${flag2} -eq 1 ]]
then
    echo "Hourly data is ready"
    exit 1
else
    echo "Hourly data is not ready"
    exit 0
fi
