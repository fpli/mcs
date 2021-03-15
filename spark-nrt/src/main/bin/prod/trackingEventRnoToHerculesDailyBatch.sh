#!/usr/bin/env bash

set -x

DT=$(date +%Y-%m-%d -d "`date` - 1 day")
DONE_FILE_DT=$(date +%Y%m%d -d "`date` - 1 day")
#DT=$1
#DONE_FILE_DT=$2

RNO_DAILY_DONE_FILE_PATH='viewfs://apollo-rno/apps/b_marketing_tracking/unified_tracking/daily_done_files'
HERCULES_DAILY_DONE_FILE_PATH='hdfs://hercules/apps/b_marketing_tracking/unified_tracking/daily_done_files'
DONE_FILE='tracking_event_'${DONE_FILE_DT}'.done'

echo "RNO_DAILY_DONE_FILE_PATH=${RNO_DAILY_DONE_FILE_PATH}"
echo "HERCULES_DAILY_DONE_FILE_PATH=${HERCULES_DAILY_DONE_FILE_PATH}"
echo "DONE_FILE=${DONE_FILE}"

/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -test -e ${HERCULES_DAILY_DONE_FILE_PATH}/${DONE_FILE}
if [ $? -eq 0 ] ;then
    echo "${DONE_FILE} Done file on Hercules has been already generated!"
    exit 0
fi

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -test -e ${RNO_DAILY_DONE_FILE_PATH}/${DONE_FILE}
if [ $? -ne 0 ] ;then
    CURRENT_DT_HOUR=$(date +%Y-%m-%d' '%H:%M:%S -d "`date`")
    CURRENT_TIMESTAMP=$(date +%s -d "$CURRENT_DT_HOUR")000

    ALERT_DT_HOUR=$(date +%Y-%m-%d' '03:00:00 -d "`date`")
    ALERT_TIMESTAMP=$(date +%s -d "$ALERT_DT_HOUR")000

    echo "CURRENT_TIMESTAMP=${CURRENT_TIMESTAMP}"
    echo "ALERT_TIMESTAMP=${ALERT_TIMESTAMP}"

    if [[ ${CURRENT_TIMESTAMP} -gt ${ALERT_TIMESTAMP} ]]; then
         echo -e "${DONE_FILE} Done file on Apollo-RNO not generated, please check!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "TRACKING EVENT Apollo-RNO ${DT} delayed!!!" -v DL-eBay-Chocolate-GC@ebay.com
         exit 1
    fi

    echo "${DONE_FILE} Done file on Apollo-RNO not generated! Keep waiting..."
    exit 0
fi

RNO_PATH='hdfs://apollo-rno/apps/b_marketing_tracking/unified_tracking/tracking_event/dt='${DT}
HERCULES_PATH='hdfs://hercules/sys/edw/imk/im_tracking/mkt_unified_tracking/tracking_event/snapshot'
ENV_PATH='/datashare/mkttracking/tools/cake'

echo "DT=${DT}"
echo "RNO_PATH=${RNO_PATH}"
echo "HERCULES_PATH=${HERCULES_PATH}"
echo "ENV_PATH=${ENV_PATH}"

JOB_NAME='CopyTrackingEventFromRnoToHerculesDailyBatchJob'

/datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}

if [ $? -ne 0 ]; then
    echo -e "Tracking_event Apollo-RNO to Hercules ${DT}'s data delayed due to sending file error!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "TRACKING EVENT HERCULES ${DT} delayed!!!" -v DL-eBay-Chocolate-GC@ebay.com
    exit 1
fi

/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop dfs -touchz "${HERCULES_DAILY_DONE_FILE_PATH}/${DONE_FILE}"

if [ $? -ne 0 ]
then
    echo -e "Failed to touch ${DT} done file on Hercules, please check!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "TRACKING EVENT HERCULES ${DT} delayed!!!" -v DL-eBay-Chocolate-GC@ebay.com
    exit 1
else
    echo -e "Congrats, tracking_event Apollo-RNO to Hercules ${DT}'s data completed" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "TRACKING EVENT HERCULES ${DT} completed" -v DL-eBay-Chocolate-GC@ebay.com
    exit 0
fi