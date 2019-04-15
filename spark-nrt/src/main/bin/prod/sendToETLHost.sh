#!/bin/bash

usage="Usage: sendToETLHost.sh"

BRIDGE_PATH=$1
INPUT_FILE=$2
log_file=$3

echo "NRT_PATH=$NRT_PATH"
echo "INPUT_FILE=$INPUT_FILE"



############### We are using epnnrt server to connect ETL server ########
BRIDGE_HOST=stack@lvsnrt2batch-1761265
BRIGE_TOKEN=/datashare/mkttracking/tools/rsa_token/id_rsa
############### ETL server for Rotation ########
ETL_HOST=etl_epn_nrt_push@phxdpeetl009.phx.ebay.com
ETL_PATH=/dw/etl/home/prod/land/dw_ams/nrt
ETL_TOKEN=/home/stack/nrt_etl_key


############################################# Send To NRT Server #####################################################
echo "sendToNrtBatchHost started~"
scp -i ${BRIGE_TOKEN} ${INPUT_FILE} ${BRIDGE_HOST}:${BRIDGE_PATH}/
rc=$?
if [[ $rc != 0 ]]; then
   echo "================== sendToNrtBatchHost ERROR!! ${INPUT_FILE} =======================" | tee -a ${log_file}
   exit $rc
else
   echo "================== sendToNrtBatchHost Completed ${INPUT_FILE} ==================="  | tee -a ${log_file}
fi

############################################# Send To ETL Server #####################################################
ssh -i ${BRIGE_TOKEN} ${BRIDGE_HOST} -t "cd $BRIDGE_PATH; bash --login " <<EOSSH
scp -i ${ETL_TOKEN} ${BRIDGE_PATH}/${INPUT_FILE} ${ETL_HOST}:${ETL_PATH}
EOSSH

rc=$?
if [[ $rc != 0 ]]; then
   echo "=================== sendToETLHost ERROR!!==================" | tee -a ${log_file}
   exit $rc
else
   echo "=================== sendToETLHost is completed ================"  | tee -a ${log_file}
   exit 0
fi
