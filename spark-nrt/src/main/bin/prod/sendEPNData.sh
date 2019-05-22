#!/usr/bin/env bash


ETL_HOST=etl_epn_nrt_push@lvsdpeetl015.lvs.ebay.com
ETL_PATH=/dw/etl/home/prod/land/dw_ams/nrt_test
ETL_TOKEN=/datashare/mkttracking/tools/rsa_token/nrt_etl_key
RENO_DIR=/apps/b_marketing_tracking/chocolate/epnnrt
log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt/send_EPN_Data${log_dt}.log"

echo `date`"=====================================================Send EPN Data started======================================================" | tee -a ${log_file}


./sendDataToRenoByMeta.sh /apps/tracking-events-workdir EPN epnnrt_scp_click meta.epnnrt_reno ${RENO_DIR} ${log_file}
rcode_click=$?

echo `date`"=====================================================Send EPN Click To Reno Finished! ======================================================" | tee -a ${log_file}


./sendDataToRenoByMeta.sh /apps/tracking-events-workdir EPN epnnrt_scp_imp meta.epnnrt_reno ${RENO_DIR} ${log_file}
rcode_imp=$?

echo `date`"=====================================================Send EPN Impression To Reno Finished! ======================================================" | tee -a ${log_file}

if [[ $rcode_click == 0 && $rcode_imp == 0 ]]; then
    echo `date`"=====================================================Send EPN Data Finished! ======================================================" | tee -a ${log_file}
    echo -e "Send EPN Data Successfully!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "Send NRT Data successfully!" -v huiclu@ebay.com | tee -a ${log_file}
fi


