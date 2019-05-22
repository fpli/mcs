#!/usr/bin/env bash
####################################################################################################################
# epnnrt done file generation script. Unless we meet all the criteria below, epnnrt is not done
# 1. Today's data are being generated
# 2. Message timestamp lack are lower than 15min
####################################################################################################################
log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt/done_${log_dt}.log"
done_file_dir=/datashare/mkttracking/data/epn-nrt/done
DT_TODAY=$(date +%Y-%m-%d)
DONE_FILE="epn_$(date +%Y%m%d -d "`date` - 1 day").done"

echo "check if done file has been generated" | tee -a ${log_file}
if [ ! -f ${done_file_dir}'/'${DONE_FILE} ]; then
    exit 0
fi

######################################### Check the file of today is processing ################################
LOCAL_PATH=/datashare/mkttracking/data/epn-nrt/process/${DT_TODAY}

today_processed=`ls ${LOCAL_PATH}'.processed' | wc -l`

if [[ today_processed -ne 1 ]]; then
     echo -e "chocolate-ePN ${DT}'s NRT not generated!!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "NRT delayed!!!!(Today's Files not generated)" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
     exit 1
fi

######################################### Check message lag from SinkAndDedupe job ################################
# 15 min
LAG_THRESHOLD=900000
echo "the threshold of kafka message: "${LAG_THRESHOLD} | tee -a ${log_file}
LAST_TS_PATH=/apps/tracking-events-workdir/last_ts/EPN/*
last_ts=`hdfs dfs -cat ${LAST_TS_PATH} | sort -n | head -1`
echo "timestamp of last message: "${last_ts} | tee -a ${log_file}
now_ts=$(($(date +%s%N)/1000000))
echo "timestamp of now: "${now_ts} | tee -a ${log_file}
message_lag=$(($now_ts-$last_ts))
echo "lag of message: "${message_lag} | tee -a ${log_file}
if [[ message_lag -gt ${LAG_THRESHOLD} ]]; then
     echo -e "chocolate-ePN ${DT}'s NRT not generated because of message lag!!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "NRT delayed!!!!(Message lag)" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
     exit 1
fi

################ Generate Done file and send it to ETL ################################
#DONE_FILE="epn_$(date +%Y%m%d -d "`date` - 1 day").done"
touch "$DONE_FILE"

/datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/sendDoneFile.sh ${DONE_FILE} ${log_file}

if [ $? -ne 0 ]; then
    echo -e "chocolate EPN NRT ${DT}'s data delayed due to sending done file error!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "EPN NRT ${DT} delayed!!!" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit 1
else
    echo -e "Congrats, chocolate EPN NRT ${DT}'s data completed" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "EPN NRT ${DT} completed" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    touch "${done_file_dir}'/'${DONE_FILE}"
    exit 0
fi

