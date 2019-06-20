#!/usr/bin/env bash

ETL_HOST=etl_epn_nrt_push@lvsdpeetl015.lvs.ebay.com
ETL_PATH=/dw/etl/home/prod/land/dw_ams/nrt_test
ETL_TOKEN=/datashare/mkttracking/tools/rsa_token/nrt_etl_key
RENO_DIR=/apps/b_marketing_tracking/chocolate/epnnrt
HECULERS_DIR=/apps/b_marketing_tracking/AMS
log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt/send_EPN_Data${log_dt}.log"
DT_TODAY=$(date +%Y-%m-%d)


echo `date +%Y-%m-%d-%H` "================================ Start sending EPN Data to ETL ================================" | tee -a ${log_file}

./scpDataToETLByMeta.sh /apps/tracking-events-workdir EPN epnnrt_scp_click meta.epnnrt_etl ${ETL_TOKEN} ${ETL_HOST}:${ETL_PATH} NO
rcode_send_etl_click=$?

if [ $rcode_send_etl_click -eq 0 ];
then
    echo "Successfully send EPN NRT Click data to ETL" | tee -a ${log_file}
else
    echo -e "Send EPN NRT Click Data To ETL Error!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending click data to ETL!" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit $rcode_send_etl_click
fi

./scpDataToETLByMeta.sh /apps/tracking-events-workdir EPN epnnrt_scp_imp meta.epnnrt_etl ${ETL_TOKEN} ${ETL_HOST}:${ETL_PATH} NO
rcode_send_etl_imp=$?

if [ $rcode_send_etl_imp -eq 0 ];
then
    echo "Successfully send EPN NRT Impression data to ETL" | tee -a ${log_file}
else
    echo -e "Send EPN NRT Impression Data To ETL Error!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending impression data to ETL!" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit $rcode_send_etl_imp
fi

echo `date +%Y-%m-%d-%H` "========================================= Successfully sending EPN Data to ETL =========================================" | tee -a ${log_file}


echo `date +%Y-%m-%d-%H` "======================================== Start sending EPN Data to Apollo Reno ========================================" | tee -a ${log_file}

./sendDataToRenoByMeta.sh /apps/tracking-events-workdir EPN epnnrt_scp_click meta.epnnrt_reno ${RENO_DIR} click YES ${log_file}
rcode_click=$?

if [ $rcode_click -eq 0 ];
then
    echo "Successfully send EPN NRT Click data to Apollo Reno" | tee -a ${log_file}
else
    echo -e "Send EPN NRT Click Data To Apollo Reno Error!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending click data to Apollo Reno!" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit $rcode_click
fi

./sendDataToRenoByMeta.sh /apps/tracking-events-workdir EPN epnnrt_scp_imp meta.epnnrt_reno ${RENO_DIR} imp YES ${log_file}
rcode_imp=$?

if [ $rcode_imp -eq 0 ];
then
    echo "Successfully send EPN NRT Impression data to Apollo Reno" | tee -a ${log_file}
else
    echo -e "Send EPN NRT Impression Data To Apollo Reno Error!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending impression data to Apollo Reno!" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit $rcode_imp
fi

echo `date +%Y-%m-%d-%H` "===================================== Successfully sending EPN Data to Apollo Reno =====================================" | tee -a ${log_file}


echo `date +%Y-%m-%d-%H`"============================ Start sending EPN Impression data from Apollo Reno to Heculers ============================" | tee -a ${log_file}

./sendDataFromRenoToHeculers.sh /apps/tracking-events-workdir EPN epnnrt_scp_click meta.epnnrt_reno ${RENO_DIR} ${HECULERS_DIR} imp ${log_file}
rcode_imp=$?

if [ $rcode_imp -eq 0 ];
then
    echo "Successfully send EPN NRT Impression Data from Apollo Reno to Heculers" | tee -a ${log_file}
else
    echo -e "Send EPN NRT Impression Data from Apollo Reno to Heculers Error!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending impression data to Apollo Reno!" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit $rcode_imp
fi

echo `date +%Y-%m-%d-%H` "======================== Successfully sending EPN Impression Data from Apollo Reno to Heculers ========================" | tee -a ${log_file}


echo `date +%Y-%m-%d-%H` "=============== Start sending EPN Click Data from Apollo Reno to Heculers and generate hourly done file ===============" | tee -a ${log_file}

./checkAmsHourlyDone.sh /apps/tracking-events-workdir EPN epnnrt_scp_click meta.epnnrt_reno
rcode_check=$?

if [$rcode_check -eq 1 ];
then
    echo "Hourly data is ready" | tee -a ${log_file}
else
    echo "Hourly data is not ready" | tee -a ${log_file}

./sendDataFromRenoToHeculers.sh /apps/tracking-events-workdir EPN epnnrt_scp_click meta.epnnrt_reno ${RENO_DIR} ${HECULERS_DIR} click ${log_file}
rcode_click=$?

if [ $rcode_click -eq 0 ];
then
    echo "Successfully send EPN NRT click data from Apollo Reno to Heculers" | tee -a ${log_file}
    if [$rcode_check -eq 1];
    then
    DONE_FILE="ams_click_hourly.done.$(date +%Y%m%d%H -d "`date` - 1 hour")00000000"
    touch "$DONE_FILE"
    /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/sendToHerculers.sh ${DONE_FILE} ${log_file}
    echo "Generate hourly done file Successfully!" | tee -a ${log_file}
else
    echo -e "Send EPN NRT click Data from Apollo Reno to Heculers failed!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending impression data from Apollo Reno to Heculers!" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit $rcode_click
fi

echo `date +%Y-%m-%d-%H` "============ Successfully sending EPN Click Data from Apollo Reno to Heculers and generate hourly done file ============" | tee -a ${log_file}
