#!/usr/bin/env bash

WORK_DIR=/apps/tracking-events-workdir
CHANNEL=EPN
USAGE_CLICK=epnnrt_scp_click
USAGE_IMP=epnnrt_scp_imp
META_SUFFIX=.epnnrt_hercules

HERCULES_DIR=/sys/edw/imk/im_tracking/epn

LOCAL_DONE_DATE_FILE_CLICK=/datashare/mkttracking/data/epn-nrt/local_done_date_click.txt
LOCAL_DONE_DATE_FILE_IMP=/datashare/mkttracking/data/epn-nrt/local_done_date_imp.txt


########################### Send EPN Click Data to Hercules and generate hourly done file ###########################

echo "================ Send EPN click data to Hercules and touch hourly done file ================"

./checkAmsHourlyDone.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${META_SUFFIX} ${LOCAL_DONE_DATE_FILE_CLICK}
rcode_check_click=$?

hercules_click_dir=${HERCULES_DIR}'/ams_click/snapshot/click_dt='
./sendDataToRenoOrHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${META_SUFFIX} ${hercules_click_dir} hercules NO
rcode_click=$?

if [ $rcode_click -eq 0 ];
then
    echo "Successfully send EPN NRT click data to Hercules"
    if [ $rcode_check_click -eq 1 ];
    then
        ## Check click done hour
        last_done_click=`cat ${LOCAL_DONE_DATE_FILE_CLICK}`
        echo "Last click done: "${last_done_click}
        last_ts_click=`date -d "${last_done_click:0:8} ${last_done_click:8}" +%s`
        let current_ts_click=${last_ts_click}+3600
        current_done_click=`date -d @${current_ts_click} "+%Y%m%d%H"`
        echo "Current click done: "${current_done_click}
        done_file_click="ams_click_hourly.done.${current_done_click}00000000"

        echo "=================== Start touching click hourly done file: ${done_file_click} ==================="
        ./touchAmsHourlyDone.sh ${done_file_click} ${LOCAL_DONE_DATE_FILE_CLICK}
    fi
else
    echo -e "Failed to send EPN NRT click data to Hercules!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in sending click data to Hercules!!!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_click
fi


######################################## Send EPN Impression Data to Hecules ########################################

echo "============= Send EPN impression data to Hercules and touch hourly done file ============="

./checkAmsHourlyDone.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${META_SUFFIX} ${LOCAL_DONE_DATE_FILE_IMP}
rcode_check_imp=$?

hercules_imp_dir=${HERCULES_DIR}'/ams_impression/snapshot/imprsn_dt='
./sendDataToRenoOrHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${META_SUFFIX} ${hercules_imp_dir} hercules NO
rcode_imp=$?

if [ $rcode_imp -eq 0 ];
then
    echo "Successfully send EPN NRT impression data to Hercules"
    if [ $rcode_check_imp -eq 1 ];
    then
        ## Check impression done hour
        last_done_imp=`cat ${LOCAL_DONE_DATE_FILE_IMP}`
        echo "Last impression done: "${last_done_imp}
        last_ts_imp=`date -d "${last_done_imp:0:8} ${last_done_imp:8}" +%s`
        let current_ts_imp=${last_ts_imp}+3600
        current_done_imp=`date -d @${current_ts_imp} "+%Y%m%d%H"`
        echo "Current impression done: "${current_done_imp}
        done_file_imp="ams_imprsn_hourly.done.${current_done_imp}00000000"

        echo "================= Start touching impression hourly done file: ${done_file_imp} ================="
        ./touchAmsHourlyDone.sh ${done_file_imp} ${LOCAL_DONE_DATE_FILE_IMP}
    fi
else
    echo -e "Failed to send EPN NRT impression data to Hercules!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in sending impression data to Hercules!!!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_imp
fi