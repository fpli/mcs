#!/usr/bin/env bash

set -x

DT=$(date +%Y-%m-%d -d "`date` - 1 day")
DONE_FILE_DT=$(date +%Y%m%d -d "`date` - 1 day")

RNO_DAILY_DONE_FILE_PATH='viewfs://apollo-rno/apps/b_marketing_tracking/unified_tracking/daily_done_files'
DONE_FILE='tracking_event_'${DONE_FILE_DT}'.done'

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -test -e ${RNO_DAILY_DONE_FILE_PATH}/${DONE_FILE}
if [ $? -eq 0 ] ;then
    echo "${DONE_FILE} Done file on Rno has been already generated!"
    exit 0
fi

RNO_DATA_FILE_PATH='viewfs://apollo-rno/apps/b_marketing_tracking/unified_tracking/tracking_event'
LOCAL_MODIFICATION_TS_FILE='modification_ts'

cur_dir=$(dirname "$0")
cur_dir=$(cd "${cur_dir}">/dev/null; pwd)

last_modification_ts=$(cat "${cur_dir}/${LOCAL_MODIFICATION_TS_FILE}")
current_modification_ts=$(/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -stat %Y "${RNO_DATA_FILE_PATH}/dt=${DT}")

if [ "${last_modification_ts}" == "${current_modification_ts}" ]; then
    echo "It's time to generate done file on Rno!"
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -touchz ${RNO_DAILY_DONE_FILE_PATH}/${DONE_FILE}
    echo ${current_modification_ts} > "${cur_dir}/${LOCAL_MODIFICATION_TS_FILE}"
    exit 0
else
    echo ${current_modification_ts} > "${cur_dir}/${LOCAL_MODIFICATION_TS_FILE}"
fi

CURRENT_DT_HOUR=$(date +%Y-%m-%d' '%H:%M:%S -d "`date`")
CURRENT_TIMESTAMP=$(date +%s -d "$CURRENT_DT_HOUR")000

ALERT_DT_HOUR=$(date +%Y-%m-%d' '01:00:00 -d "`date`")
ALERT_TIMESTAMP=$(date +%s -d "$ALERT_DT_HOUR")000

echo "CURRENT_TIMESTAMP=${CURRENT_TIMESTAMP}"
echo "ALERT_TIMESTAMP=${ALERT_TIMESTAMP}"

if [[ ${CURRENT_TIMESTAMP} -gt ${ALERT_TIMESTAMP} ]]; then
     echo -e "${DONE_FILE} Done file on Apollo-RNO not generated, please check!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "TRACKING EVENT Apollo-RNO ${DT} delayed!!!" -v DL-eBay-Chocolate-GC@ebay.com
     exit 1
fi

echo "${DONE_FILE} Done file on Apollo-RNO not generated! Keep waiting..."