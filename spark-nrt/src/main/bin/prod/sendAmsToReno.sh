#!/usr/bin/env bash

WORK_DIR=/apps/tracking-events-workdir
CHANNEL=EPN
USAGE_CLICK=epnnrt_scp_click
USAGE_IMP=epnnrt_scp_imp
META_SUFFIX=.epnnrt_reno

RENO_DIR=/apps/b_marketing_tracking/chocolate/epnnrt


######################################### Send EPN Click Data to Apollo Reno #########################################

echo "================ Send EPN Click Data to Apollo Reno ================"

./sendDataToRenoThenToHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${META_SUFFIX} ${RENO_DIR} click YES
rcode_click=$?

if [ $rcode_click -eq 0 ];
then
    echo "Successfully send AMS Click data to Apollo Reno"
else
    echo -e "Send EPN NRT Click Data from Apollo Reno to Hercules failed!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending impression data from Apollo Reno to Hercules!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_click
fi


####################################### Send EPN Impression Data to Apollo Reno #######################################

echo "================ Send EPN Impression Data to Apollo Reno ================"

./sendDataToRenoThenToHerculesByMeta.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${META_SUFFIX} ${RENO_DIR} imp YES
rcode_imp=$?

if [ $rcode_imp -eq 0 ];
then
    echo "Successfully send AMS Impression data to Hercules"
else
    echo -e "Send EPN NRT Impression Data from Apollo Reno to Hercules failed!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[EPN NRT ERROR] Error in sending impression data to Apollo Reno!" -v DL-eBay-Chocolate-GC@ebay.com
    exit $rcode_imp
fi