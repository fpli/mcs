#!/usr/bin/env bash

WORK_DIR=/apps/tracking-events-workdir
CHANNEL=EPN
USAGE_CLICK=epnnrt_scp_click
USAGE_IMP=epnnrt_scp_imp
META_SUFFIX=.epnnrt_hercules

HERCULES_DIR=/sys/edw/imk/im_tracking/epn

LOCAL_DONE_DATE_FILE=/datashare/mkttracking/data/epn-nrt/local_done_date.txt


########################### Send EPN Click Data to Hercules and generate hourly done file ###########################

echo "================ Send EPN Click Data to Hercules and generate hourly done file ================"

./checkAmsHourlyDone.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${META_SUFFIX} ${LOCAL_DONE_DATE_FILE}
rcode_check=$?

hercules_click_dir=${HERCULES_DIR}'/ams_click/snapshot/click_dt='
./sendDataToRenoOrHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${META_SUFFIX} ${hercules_click_dir} hercules NO
rcode_click=$?

if [ $rcode_click -eq 0 ];
then
    echo "Successfully send EPN NRT Click Data to Hercules"
    if [ $rcode_check -eq 1 ];
    then
        ## Check done hour
        last_done=`cat ${LOCAL_DONE_DATE_FILE}`
        echo "last done: "${last_done}
        last_ts=`date -d "${last_done:0:8} ${last_done:8}" +%s`
        let current_ts=${last_ts}+3600
        current_done=`date -d @${current_ts} "+%Y%m%d%H"`
        echo "current done: "${current_done}
        done_file="ams_click_hourly.done.${current_done}00000000"

        echo "============== Start generating hourly done file: ${done_file} =============="
        ./touchAmsHourlyDone.sh ${done_file} ${LOCAL_DONE_DATE_FILE}
    fi
else
    echo -e "Send EPN NRT Click Data to Hercules failed!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Send click file to Hercules Failed!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_click
fi


######################################## Send EPN Impression Data to Hecules ########################################

echo "================ Send EPN Impression Data to Hecules ================"

hercules_imp_dir=${HERCULES_DIR}'/ams_impression/snapshot/imprsn_dt='
./sendDataToRenoOrHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${META_SUFFIX} ${hercules_imp_dir} hercules NO
rcode_imp=$?

if [ $rcode_imp -eq 0 ];
then
    echo "Successfully send EPN NRT Impression data to Hercules"
else
    echo -e "Send EPN NRT Impression Data to Hercules failed!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Send impression file to Hercules Failed!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_imp
fi