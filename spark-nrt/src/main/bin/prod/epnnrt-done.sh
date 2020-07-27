#!/usr/bin/env bash
####################################################################################################################
# epnnrt done file generation script. Unless we meet all the criteria below, epnnrt is not done
# 1. Today's data are being generated
# 2. Message timestamp less than start timestamp of current date
####################################################################################################################
log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt/done_${log_dt}.log"
done_file_dir="/datashare/mkttracking/data/epn-nrt/done"
DT_TODAY=$(date +%Y-%m-%d)
DONE_FILE="epn_$(date +%Y%m%d -d "`date` - 1 day").done"
LOCAL_PATH=/datashare/mkttracking/data/epn-nrt/etl/"date="${DT_TODAY}

echo "check if done file has been generated"
if [ -f "${done_file_dir}/${DONE_FILE}" ]; then
    echo "${DONE_FILE} Done file has been already generated!" | tee -a ${log_file}
    exit 0
fi

######################################### Check the file of today is processing ################################
today_processed_click=`ls ${LOCAL_PATH}'.click.processed' | wc -l`
today_processed_impression=`ls ${LOCAL_PATH}'.impression.processed' | wc -l`

if [[ today_processed_click -ne 1 ]]; then
     echo -e "chocolate-ePN ${DT_TODAY}'s NRT CLICK not generated!!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "NRT delayed!!!!(Today's Files not generated)" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
     exit 1
fi

if [[ today_processed_impression -ne 1 ]]; then
     echo -e "chocolate-ePN ${DT_TODAY}'s NRT IMPRESSION not generated!!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "NRT delayed!!!!(Today's Files not generated)" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
     exit 1
fi

######################################### Check message lag from SinkAndDedupe job ################################
# modified by zhofan on 2020-03-01. Every time check the min timestamp of kafka message and the start timestamp of current date.
# If the start timestamp of current date is greater than the min timestamp of kafka message, then send NRT delay mail.
export HADOOP_USER_NAME=chocolate
#echo "the threshold of kafka message: "${LAG_THRESHOLD}
LAST_TS_PATH=/apps/tracking-events-workdir/last_ts/EPN/*
last_ts=`hdfs dfs -cat ${LAST_TS_PATH} | sort -n | head -1`
echo "timestamp of last message: "${last_ts}
DT_HOUR_TODAY=$(date +%Y-%m-%d' '00:00:00 -d "`date`")
TIMESTAMP_TODAY=$(date +%s -d "$DT_HOUR_TODAY")000
#now_ts=$(($(date +%s%N)/1000000))
echo "start timestamp of current date: "${TIMESTAMP_TODAY}
#message_lag=$(($now_ts-$last_ts))
#echo "lag of message: "${message_lag}
if [[ ${TIMESTAMP_TODAY} -gt ${last_ts} ]]; then
     echo -e "chocolate-ePN ${DT_TODAY}'s NRT not generated because of message lag!!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "NRT delayed!!!!(Message lag)" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
     exit 1
fi

################ Generate Done file and send it to ETL ################################
#DONE_FILE="epn_$(date +%Y%m%d -d "`date` - 1 day").done"
touch "$DONE_FILE"

/datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/sendDoneFile.sh ${DONE_FILE} ${log_file}

if [ $? -ne 0 ]; then
    echo -e "chocolate EPN NRT ${DT_TODAY}'s data delayed due to sending done file error!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "EPN NRT ${DT_TODAY} delayed!!!" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit 1
else
    echo -e "Congrats, chocolate EPN NRT ${DT_TODAY}'s data completed" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "EPN NRT ${DT_TODAY} completed" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    #echo -e "Hello world, Today is my first day to send chocolate EPN NRT ${DT_TODAY}'s data completed to you! Nice to meet you ^^" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "Hello World! EPN NRT ${DT_TODAY} completed" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    touch "${done_file_dir}/${DONE_FILE}"
    exit 0
fi