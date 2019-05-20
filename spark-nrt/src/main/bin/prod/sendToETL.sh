#!/bin/bash

usage="Usage: sendToETL.sh"

INPUT_FILE=$1
log_file=$2

echo "INPUT_FILE=$INPUT_FILE"

############### ETL server for EPN NRT ########
ETL_HOST=etl_epn_nrt_push@lvsdpeetl015.lvs.ebay.com
ETL_PATH=/dw/etl/home/prod/land/dw_ams/nrt
ETL_TOKEN=/datashare/mkttracking/tools/rsa_token/nrt_etl_key

############################################# Send To ETL Server #####################################################
echo "sendToETL started~"

#max 3 times copy data to ETL
retry=1
rcode=1

until [[ ${retry} -gt 3 ]]
do
    scp -i ${ETL_TOKEN} ${INPUT_FILE} ${ETL_HOST}:${ETL_PATH}/
    rcode=$?
    if [ ${rcode} -eq 0 ]
    then
        break
    else
        echo "Faild to send data to ETL, retrying ${retry}"
        retry=`expr ${retry} + 1`
    fi
done

if [[ $rcode -ne 0 ]]; then
   echo "Chocolate EPN NRT file sendToETL ERROR!!" | mail -s "SendToETL ERROR!!!!" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
   exit $rcode
else
   echo "=================== sendToETL is completed ================"  | tee -a ${log_file}
   exit 0
fi
