#!/bin/bash

usage="Usage: sendToETLHost.sh"

DT=$(date +%Y-%m-%d -d "`date`")
if [ $# -eq 1 ]; then
  DT_HOUR=$(date +%Y-%m-%d_$1 -d "`date`")
else
  DT_HOUR=$(date +%Y-%m-%d_%H -d "`date`")
fi
INPUT_PATH=/mnt/chocolate/rotation/teradata/dt=${DT}
log_file="/mnt/chocolate/rotation/logs/toTD_$DT_HOUR_$START_TIME.log"

echo "DT_HOUR=$DT_HOUR"
echo "INPUT_PATH=$INPUT_PATH"

echo "sendToNrtBatchHost started~"

echo "start uploading..."
sftp -i /home/yimeng/.ssh/id_rsa_nrt yimeng@lvsnrt2batch-2236360.stratus.lvs.ebay.com <<EOSFTP
cd /home/yimeng/chocolate/rotation/teradata
if [ ! -d dt=${DT} ]; then
 mkdir dt=${DT}
fi
cd dt=${DT}
put ${INPUT_PATH}/${DT_HOUR}*
EOSFTP
rc=$?
if [[ $rc != 0 ]]; then
   echo "=====================================================sendToNrtBatchHost ERROR!!======================================================"
   echo "=====================================================sendToNrtBatchHost ERROR!!======================================================"  >> ${log_file}
   exit $rc
else
   echo "=====================================================sendToNrtBatchHost is completed-======================================================"
   echo "=====================================================sendToNrtBatchHost is completed-======================================================"  >> ${log_file}
fi

ssh -i /home/yimeng/.ssh/id_rsa_nrt yimeng@lvsnrt2batch-2236360.stratus.lvs.ebay.com <<EOSSH
sftp -i /home/yimeng/.ssh/id_rsa_etl yimeng@phxdpeetl011.phx.ebay.com:/dw/etl/home/prod/land/dw_coreimk/rotation
put /home/yimeng/chocolate/rotation/teradata/dt=${DT}/${DT_HOUR}*
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
