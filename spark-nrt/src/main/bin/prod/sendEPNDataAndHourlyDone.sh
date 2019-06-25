#!/usr/bin/env bash

WORK_DIR=/apps/tracking-events-workdir
CHANNEL=EPN
USAGE_CLICK=epnnrt_scp_click
USAGE_IMP=epnnrt_scp_imp
META_SUFFIX_ETL=.epnnrt_etl
META_SUFFIX_RNO=.epnnrt_reno

ETL_HOST=etl_epn_nrt_push@lvsdpeetl015.lvs.ebay.com
ETL_PATH=/dw/etl/home/prod/land/dw_ams/nrt_test
ETL_TOKEN=/datashare/mkttracking/tools/rsa_token/nrt_etl_key
RENO_DIR=/apps/b_marketing_tracking/chocolate/epnnrt
HERCULES_DIR=/apps/b_marketing_tracking/AMS

log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt/send_EPN_Data${log_dt}.log"
DT_TODAY=$(date +%Y-%m-%d)


################################################ Send EPN Data to ETL ################################################

echo "============== Send EPN Data to ETL =============="

./scpDataToETLByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${META_SUFFIX_ETL} ${ETL_TOKEN} ${ETL_HOST}:${ETL_PATH} NO
rcode_send_etl_click=$?

if [ $rcode_send_etl_click -eq 0 ];
then
    echo "Successfully send EPN NRT Click data to ETL" | tee -a ${log_file}
else
    echo -e "Send EPN NRT Click Data To ETL Error!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending click data to ETL!" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit $rcode_send_etl_click
fi

./scpDataToETLByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${META_SUFFIX_ETL} ${ETL_TOKEN} ${ETL_HOST}:${ETL_PATH} NO
rcode_send_etl_imp=$?

if [ $rcode_send_etl_imp -eq 0 ];
then
    echo "Successfully send EPN NRT Impression data to ETL" | tee -a ${log_file}
else
    echo -e "Send EPN NRT Impression Data To ETL Error!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending impression data to ETL!" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit $rcode_send_etl_imp
fi


################## Send EPN Click Data to Apollo Reno then to Hercules and generate hourly done file ##################

echo "============== Send EPN Click Data to Apollo Reno then to Hercules and generate hourly done file =============="

./checkAmsHourlyDone.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${META_SUFFIX_RNO}
rcode_check=$?

./sendDataToRenoThenToHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${META_SUFFIX_RNO} ${RENO_DIR} ${HERCULES_DIR} click YES
rcode_click=$?

if [ $rcode_click -eq 0 ];
then
    echo "Successfully send EPN NRT Click Data from Apollo Reno to Hercules"
    if [$rcode_check -eq 1];
    then
        done_file="ams_click_hourly.done.$(date +%Y%m%d%H -d "`date` - 1 hour")00000000"
        ./generateHourlyDoneFile ${done_file}
    else
        exit $rcode_check
    fi
else
    echo -e "Send EPN NRT Click Data from Apollo Reno to Hercules failed!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending impression data from Apollo Reno to Hercules!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_click
fi


############################### Send EPN Impression Data to Apollo Reno then to Hecules ###############################

echo "============== Send EPN Impression Data to Apollo Reno then to Hecules =============="

./sendDataToRenoThenToHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${META_SUFFIX_RNO} ${RENO_DIR} ${HERCULES_DIR} imp YES
rcode_imp=$?

if [ $rcode_imp -eq 0 ];
then
    echo "Successfully send EPN NRT Impression data from Apollo Reno to Hercules"
else
    echo -e "Send EPN NRT Impression Data from Apollo Reno to Hercules failed!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending impression data to Apollo Reno!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_imp
fi