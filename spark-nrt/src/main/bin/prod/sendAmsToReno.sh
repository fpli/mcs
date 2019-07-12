#!/usr/bin/env bash

WORK_DIR=/apps/tracking-events-workdir
CHANNEL=EPN
USAGE_CLICK=epnnrt_scp_click
USAGE_IMP=epnnrt_scp_imp
META_SUFFIX=.epnnrt_reno

RENO_DIR=/apps/b_marketing_tracking/chocolate/epnnrt


######################################### Send EPN Click Data to Apollo Reno #########################################

echo "================ Send EPN Click Data to Apollo Reno ================"

reno_click_dir=${RENO_DIR}'/click/click_dt='
./sendDataToRenoOrHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${META_SUFFIX} ${reno_click_dir} reno YES
rcode_click=$?

if [ $rcode_click -eq 0 ];
then
    echo "Successfully send AMS Click data to Apollo Reno"
else
    echo -e "Send EPN NRT Click Data to Apollo Reno failed!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Send click file to Apollo Reno!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_click
fi


####################################### Send EPN Impression Data to Apollo Reno #######################################

echo "================ Send EPN Impression Data to Apollo Reno ================"

reno_imp_dir=${RENO_DIR}'/imp/imprsn_dt='
./sendDataToRenoOrHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${META_SUFFIX} ${reno_imp_dir} reno NO
rcode_imp=$?

if [ $rcode_imp -eq 0 ];
then
    echo "Successfully send AMS Impression data to Hercules"
else
    echo -e "Send EPN NRT Impression Data to Apollo Reno failed!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Send impression file to Apollo Reno!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_imp
fi