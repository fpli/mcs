#!/bin/bash

usage="Usage: sendDoneFile.sh"

INPUT_FILE=$1
log_file=$2

echo "INPUT_FILE=$INPUT_FILE"

############### ETL server for EPN NRT ########
ETL_HOST=etl_epn_nrt_push@lvsdpeetl015.lvs.ebay.com
ETL_PATH=/dw/etl/home/prod/land/dw_ams/nrt
ETL_TOKEN=/datashare/mkttracking/tools/rsa_token/nrt_etl_key

############################################# Send To ETL Server #####################################################
echo "=================== start sending done file ================"  | tee -a ${log_file}

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
        echo "Faild to send done file to ETL, retrying ${retry}"
        retry=`expr ${retry} + 1`
    fi
done

if [[ $rcode -ne 0 ]]; then
   echo "Send Chocolate EPN NRT done file error!!" | mail -s "Send Done File ERROR!!!!" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
   exit $rcode
else
   echo "===================Successfully send done file ================"  | tee -a ${log_file}
   exit 0
fi