#!/bin/bash

usage="Usage: sendToETLHost.sh"

NRT_PATH=$1
INPUT_FILE=$2
log_file=$3

echo "NRT_PATH=$NRT_PATH"
echo "INPUT_FILE=$INPUT_FILE"

echo "sendToNrtBatchHost started~"

echo "start uploading..."
sftp -i /datashare/mkttracking/tools/rsa_token/id_rsa lvsnrt2batch-1761265 <<EOSFTP
cd ${NRT_PATH}
put ${INPUT_FILE}
exit
EOSFTP

rc=$?
if [[ $rc != 0 ]]; then
   echo "================== sendToNrtBatchHost ERROR!! ${INPUT_FILE} =======================" | tee -a ${log_file}
   exit $rc
else
   echo "================== sendToNrtBatchHost Completed ${INPUT_FILE} ==================="  | tee -a ${log_file}
fi

sftp -i /datashare/mkttracking/tools/rsa_token/id_rsa lvsnrt2batch-1761265 <<EOSSH
sftp yimeng@phxdpeetl009.phx.ebay.com:/dw/etl/home/prod/land/dw_ams/nrt
put ${NRT_PATH}${DT}${INPUT_FILE}
exit
EOSSH

rc=$?
if [[ $rc != 0 ]]; then
   echo "=====================================================sendToETLHost ERROR!!======================================================"
   echo "=====================================================sendToETLHost ERROR!!======================================================"  >> ${log_file}
   exit $rc
else
   echo "=====================================================sendToETLHost is completed-======================================================"
   echo "=====================================================sendToETLHost is completed-======================================================"  >> ${log_file}
   exit 0
fi
