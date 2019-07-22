#!/usr/bin/env bash

WORK_DIR=/apps/tracking-events-workdir
CHANNEL=EPN
USAGE_CLICK=epnnrt_scp_click
USAGE_IMP=epnnrt_scp_imp
META_SUFFIX=.epnnrt_etl

ETL_HOST=etl_epn_nrt_push@lvsdpeetl015.lvs.ebay.com
ETL_PATH=/dw/etl/home/prod/land/dw_ams/nrt_test
ETL_TOKEN=/datashare/mkttracking/tools/rsa_token/nrt_etl_key


############################################# Send EPN Click Data to ETL #############################################

echo "================================ Send EPN Click Data to ETL ================================"

./scpDataToETLByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${META_SUFFIX} ${ETL_TOKEN} ${ETL_HOST}:${ETL_PATH} NO YES
rcode_click=$?

if [ $rcode_click -eq 0 ];
then
    echo "Successfully send EPN NRT Click data to ETL"
else
    echo -e "Failed to send EPN NRT click data to ETL!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending click data to ETL!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_send_etl_click
fi


########################################### Send EPN Impression Data to ETL ###########################################

echo "============================= Send EPN Impression Data to ETL ============================="

./scpDataToETLByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${META_SUFFIX} ${ETL_TOKEN} ${ETL_HOST}:${ETL_PATH} NO NO
rcode_imp=$?

if [ $rcode_send_etl_imp -eq 0 ];
then
    echo "Successfully send EPN NRT Impression data to ETL"
else
    echo -e "Failed to send EPN NRT impression data to ETL!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending impression data to ETL!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_imp
fi
