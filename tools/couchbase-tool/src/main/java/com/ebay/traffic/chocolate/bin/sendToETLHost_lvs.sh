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

echo "DT_HOUR=$DT_HOUR" | tee -a ${log_file}
echo "INPUT_PATH=$INPUT_PATH" | tee -a ${log_file}

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

############### ETL server for Rotation ########
ETL_HOST=yimeng@lvsdpeetl012.lvs.ebay.com
ETL_PATH=/dw/etl/home/prod/land/dw_coreimk/rotation
ETL_TOKEN=/datashare/mkttracking/tools/rsa_token/id_rsa

echo "ETL_HOST=$ETL_HOST" | tee -a ${log_file}
echo "ETL_PATH=$ETL_PATH" | tee -a ${log_file}

echo "start uploading..."

chmod -R 777 ${INPUT_PATH}
cd ${INPUT_PATH}
FILE_LIST=file_lvs.txt
ls ${INPUT_PATH}/${DT_HOUR}*.txt > ${FILE_LIST}
ls ${INPUT_PATH}/${DT_HOUR}*.status >> ${FILE_LIST}

###################################### Send To ETL Server ###################################################
while read p; do
  if [[ ! -f "${p}.processed.lvs" ]]; then
    scp -i ${ETL_TOKEN} ${p} ${ETL_HOST}:${ETL_PATH}
    rc=$?
    if [[ $rc != 0 ]]; then
        echo "===================== NRT sendToETLServer ERROR!! ${p}========================"  | tee -a ${log_file}
        echo "rotation ${p} was not sent to ETL Server!!!!" | mail -s "Rotation SendToETL Error!!!!" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
        exit $rc
    else
        mv ${p} ${p}.processed.lvs
        echo "===================== NRT sendToETLServer is completed !!! ${p}/dt=${DT}/${fileName}==============="  | tee -a ${log_file}
    fi
  fi
done < ${FILE_LIST}
