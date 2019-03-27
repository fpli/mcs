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

echo "sendToNrtBatchHost started~"

echo "start uploading..."
sftp -i /home/stack/.ssh/id_rsa_nrt yimeng@lvsnrt2batch-2236360.stratus.lvs.ebay.com <<EOSFTP
cd /home/yimeng/chocolate/rotation/teradata
if [ ! -d dt=${DT} ]; then
 mkdir dt=${DT}
fi
cd dt=${DT}
put ${INPUT_PATH}/${DT_HOUR}*
EOSFTP
rc=$?
if [[ $rc != 0 ]]; then
   echo "=====================================================sendToNrtBatchHost ERROR!!======================================================"  | tee -a ${log_file}
   exit $rc
else
   echo "=====================================================sendToNrtBatchHost is completed-======================================================"  | tee -a ${log_file}
fi

ssh -i /home/stack/.ssh/id_rsa_nrt yimeng@lvsnrt2batch-2236360.stratus.lvs.ebay.com <<EOSSH
sftp -i /home/yimeng/.ssh/id_rsa_etl yimeng@phxdpeetl011.phx.ebay.com:/dw/etl/home/prod/land/dw_coreimk/rotation
put /home/yimeng/chocolate/rotation/teradata/dt=${DT}/${DT_HOUR}*
if [[ $rc != 0 ]]; then
   echo "===================== NRT sendToNrtBatchHost ERROR!! ========================"  | tee -a ${log_file}
   exit $rc
else
   echo "===================== NRT sendToNrtBatchHost is completed !!! ==============="  | tee -a ${log_file}
fi
EOSSH

rc=$?
if [[ $rc != 0 ]]; then
   echo "=====================================================sendToETLHost ERROR!!======================================================"  | tee -a ${log_file}
   exit $rc
else
   echo "=====================================================sendToETLHost is completed-======================================================"  | tee -a ${log_file}
   exit 0
fi
