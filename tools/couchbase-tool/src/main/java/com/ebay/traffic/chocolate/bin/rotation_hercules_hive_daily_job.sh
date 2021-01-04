#!/bin/bash
usage="Usage: rotation hercules daily update job"

HOST_NAME=`hostname -f`

DT=$(date +%Y-%m-%d -d "`date` -1 day")
log_file="/datashare/mkttracking/jobs/tdmoveoff/rotation/logs/hive/dt=${DT}/"
if [ ! -d ${log_file} ]; then
 mkdir ${log_file}
 chmod 777 ${log_file}
fi
log_file=${log_file}daily_${HOST_NAME}_$(date +%s).log

echo "DT="${DT} | tee -a ${log_file}
echo "LOG_FILE"=${log_file} | tee -a ${log_file}

echo `date`" =============== Hercules Rotations Table Job Start ==========="
/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_hercules_hive_related_table_update.sh rotations ${log_file}
rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Hercules Rotations Table Job error ==="
    exit $rc
else
    echo `date`" =============== Hercules Rotations Table Job End ==========="
fi

echo `date`" =============== Hercules Campaigns Table Job Start ==========="
/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_hercules_hive_related_table_update.sh campaigns ${log_file}
rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Hercules Campaigns Table Job error ==="
    exit $rc
else
    echo `date`" =============== Hercules Campaigns Table Job End ==========="
fi


echo `date`" =============== Hercules Vendors Table Job Start ==========="
/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_hercules_hive_related_table_update.sh vendors ${log_file}
rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Hercules Vendors Table Job error ==="
    exit $rc
else
    echo `date`" =============== Hercules Vendors Table Job End ==========="
fi


echo `date`" =============== Hercules Clients Table Job Start ==========="
/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_hercules_hive_related_table_update.sh clients ${log_file}
rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Hercules Clients Table Job error ==="
    exit $rc
else
    echo `date`" =============== Hercules Clients Table Job End ==========="
fi


echo `date`" =============== Hercules Done File Start =================="
TOUCH_DT=$(date +%Y%m%d -d "`date` -1 day")
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop dfs -touchz hdfs://hercules/apps/b_marketing_tracking/watch/${TOUCH_DT}/rotation_daily.done.${TOUCH_DT}
rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Hercules Done File error ==="
    exit $rc
else
    echo `date`" =============== Hercules Done File End ==========="
fi



rc=$?
if [[ $rc != 0 ]]; then
    echo " ===== rotation daily update hercules hive table END With ERROR ====="
    exit $rc
else
    echo -e " Rotation Hercules Hive table ${DT}'s data completed"  | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "Rotation Hercules ${DT} completed" -v DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
fi