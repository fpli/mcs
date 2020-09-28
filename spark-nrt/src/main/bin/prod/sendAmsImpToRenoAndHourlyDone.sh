#!/usr/bin/env bash

WORK_DIR=hdfs://elvisha/apps/tracking-events-workdir
CHANNEL=EPN
USAGE_CLICK=epnnrt_scp_click
USAGE_IMP=epnnrt_scp_imp
META_SUFFIX=.epnnrt_reno

RENO_DIR=/apps/b_marketing_tracking/chocolate/epnnrt
MID_DIR=/apps/b_marketing_tracking/chocolate/epnnrt/ams_scp_middle

LOCAL_DONE_DATE_FILE_CLICK=/datashare/mkttracking/data/epn-nrt/local_done_date_rno_click.txt
LOCAL_DONE_DATE_FILE_IMP=/datashare/mkttracking/data/epn-nrt/local_done_date_rno_imp.txt

MIN_TS_FILE_CLICK=/apps/epn-nrt/min_ts_rno_click.txt
MIN_TS_FILE_IMP=/apps/epn-nrt/min_ts_rno_imp.txt

function get_current_done(){
    last_done=`cat $1`
    last_ts=`date -d "${last_done:0:8} ${last_done:8}" +%s`
    let current_ts=${last_ts}+3600
    current_done=`date -d @${current_ts} "+%Y%m%d%H"`
    echo ${current_done}
}


####################################### Send EPN Impression Data to Apollo Reno #######################################

echo "========================= Send EPN Impression Data to Apollo Reno ========================="

./checkAmsHourlyDone.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${META_SUFFIX} ${LOCAL_DONE_DATE_FILE_IMP} ${MIN_TS_FILE_IMP}
rcode_check_imp=$?

reno_imp_dir=${RENO_DIR}'/imp/imprsn_dt='
./sendDataToRenoOrHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${META_SUFFIX} ${reno_imp_dir} ${MID_DIR} reno
rcode_imp=$?

if [ ${rcode_imp} -eq 0 ];
then
    echo "Successfully send AMS impression data to Apollo Reno"
    if [ ${rcode_check_imp} -eq 1 ];
    then
        current_done_imp=$(get_current_done ${LOCAL_DONE_DATE_FILE_IMP})

        echo "================= Start touching impression hourly done file: ${done_file_imp} ================="
        ./touchAmsHourlyDone.sh ${current_done_imp} ${LOCAL_DONE_DATE_FILE_IMP} imp reno
    fi
else
    echo -e "Failed to send EPN NRT impression data to Apollo Reno!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in sending impression data to Apollo Reno!!!" -v DL-eBay-Chocolate-GC@ebay.com
    exit ${rcode_imp}
fi