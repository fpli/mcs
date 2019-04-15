#!/bin/bash

usage="Usage: sendToETLHost.sh"

DT=$(date +%Y-%m-%d -d "`date` - 1 hour")
if [ $# -eq 1 ]; then
  DT_HOUR=$(date +%Y-%m-%d_$1 -d "`date` - 1 hour")
else
  DT_HOUR=$(date +%Y-%m-%d_%H -d "`date` - 1 hour")
fi
INPUT_PATH=/datashare/mkttracking/data/rotation/teradata/dt=${DT}
log_file="/datashare/mkttracking/logs/rotation/teradata/toETL_$DT_HOUR.log"

echo "DT_HOUR=$DT_HOUR"
echo "INPUT_PATH=$INPUT_PATH"

if [[ ! -d ${INPUT_PATH} ]]; then
   echo "======================== No rotation files was not generated !! =============================="  | tee -a ${log_file}
   echo "chocolate-rotation ${DT_HOUR}'s data is NOT generated~!!!" | mail -s "No Rotation result!!!!" DL-eBay-Chocolate-GC@ebay.com
   exit 1
fi

if [[ ! -f ${INPUT_PATH}/${DT_HOUR}_campaigns.txt || ! -f ${INPUT_PATH}/${DT_HOUR}_rotations.txt ]]; then
   echo "======================== rotation related files was not generated !! =============================="  | tee -a ${log_file}
   echo "chocolate-rotation ${DT_HOUR}'s data is NOT generated~!!!" | mail -s "No Rotation result!!!!" DL-eBay-Chocolate-GC@ebay.com
   exit 1
fi


############### We are using epnnrt server to connect ETL server ########
BRIDGE_HOST=stack@lvsnrt2batch-2236360.stratus.lvs.ebay.com
BRIDGE_PATH=/home/stack/chocolate/rotation/teradata
BRIGE_TOKEN=/datashare/mkttracking/tools/rsa_token/id_rsa

############### ETL server for Rotation ########
ETL_HOST=etl_epn_nrt_push@phxdpeetl009.phx.ebay.com
ETL_PATH=/dw/etl/home/prod/land/dw_coreimk/rotation
ETL_TOKEN=/home/stack/chocolate/rotation/nrt_etl_key


echo "sendToNrtBatchHost started~"

echo "start uploading..."

############################################# Send To NRT Server #####################################################
ssh -i ${BRIGE_TOKEN} ${BRIDGE_HOST} -t "if [[ ! -d ${BRIDGE_PATH}/dt=${DT} ]]; then  mkdir -p ${BRIDGE_PATH}/dt=${DT}; fi;"
scp -i ${BRIGE_TOKEN} ${INPUT_PATH}/${DT_HOUR}* ${BRIDGE_HOST}:${BRIDGE_PATH}/dt=${DT}
rc=$?
if [[ $rc != 0 ]]; then
   echo "======================== sendToNrtBatchHost ERROR!! =============================="  | tee -a ${log_file}
   exit $rc
else
   echo "======================= sendToNrtBatchHost is completed ========================="  | tee -a ${log_file}
fi


chmod -R 777 ${INPUT_PATH}
cd ${INPUT_PATH}
FILE_LIST=file.txt
ls ${INPUT_PATH}/${DT_HOUR}* > ${FILE_LIST}

ssh -i ${BRIGE_TOKEN} ${BRIDGE_HOST} -t "if [[ ! -d ${BRIDGE_PATH}/dt=${DT} ]]; then  mkdir -p ${BRIDGE_PATH}/dt=${DT}; fi;"

while read p; do
  if [ ! -f "${p}.processed" ]; then
    #################################### Send To NRT Server ##################################################
    scp -i ${BRIGE_TOKEN} ${INPUT_PATH}/${DT_HOUR}* ${BRIDGE_HOST}:${BRIDGE_PATH}/dt=${DT}
    rc=$?
    if [[ $rc != 0 ]]; then
        echo "=====================Local sendToNRTServer ERROR!! ${p}========================"  | tee -a ${log_file}
        echo "rotation ${p} was not sent to NRT Server!!!!" | mail -s "Rotation SendToNRT Error!!!!" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
        exit $rc
    else
        echo "=====================Local sendToNRTServer is completed !!! ${p}==============="  | tee -a ${log_file}
    fi
    ###################################### Send To ETL Server ###################################################
    idx=${#INPUT_PATH}
    fileName=${p:$idx+1}
    ssh -i ${BRIGE_TOKEN} ${BRIDGE_HOST} -t "cd ${BRIDGE_PATH}/dt=${DT}; bash --login" <<EOSSH
    scp -i ${ETL_TOKEN} ${BRIDGE_PATH}/dt=${DT}/${fileName} ${ETL_HOST}:${ETL_PATH}
EOSSH
    rc=$?
    if [[ $rc != 0 ]]; then
        echo "===================== NRT sendToETLServer ERROR!! ${BRIDGE_PATH}/dt=${DT}/${fileName}========================"  | tee -a ${log_file}
        echo "rotation ${p} was not sent to ETL Server!!!!" | mail -s "Rotation SendToETL Error!!!!" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
        exit $rc
    else
        mv ${p} ${p}.processed
        echo "===================== NRT sendToETLServer is completed !!! ${BRIDGE_PATH}/dt=${DT}/${fileName}==============="  | tee -a ${log_file}
    fi
  fi
done < ${FILE_LIST}
