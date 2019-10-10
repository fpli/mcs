#!/bin/bash
usage="Usage: rotation rno daily update job"

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

echo `date`" =============== Apollo-Rno Rotations Table Job Start ===========" | tee -a ${log_file}
/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_rno_hive_related_table_update.sh rotations ${log_file}
rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Apollo-Rno Rotations Table Job error ===" | tee -a ${log_file}
    exit $rc
else
    echo `date`" =============== Apollo-Rno Rotations Table Job End ===========" | tee -a ${log_file}
fi

echo `date`" =============== Apollo-Rno Campaigns Table Job Start ===========" | tee -a ${log_file}
/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_rno_hive_related_table_update.sh campaigns ${log_file}
rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Apollo-Rno Campaigns Table Job error ===" | tee -a ${log_file}
    exit $rc
else
    echo `date`" =============== Apollo-Rno Campaigns Table Job End ===========" | tee -a ${log_file}
fi


echo `date`" =============== Apollo-Rno Vendors Table Job Start ===========" | tee -a ${log_file}
/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_rno_hive_related_table_update.sh vendors ${log_file}
rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Apollo-Rno Vendors Table Job error ===" | tee -a ${log_file}
    exit $rc
else
    echo `date`" =============== Apollo-Rno Vendors Table Job End ===========" | tee -a ${log_file}
fi


echo `date`" =============== Apollo-Rno Clients Table Job Start ===========" | tee -a ${log_file}
/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_rno_hive_related_table_update.sh clients ${log_file}
rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Apollo-Rno Clients Table Job error ===" | tee -a ${log_file}
    exit $rc
else
    echo `date`" =============== Apollo-Rno Clients Table Job End ===========" | tee -a ${log_file}
fi


echo `date`" =============== Apollo-Rno Done File Start ==================" | tee -a ${log_file}
TOUCH_DT=$(date +%Y%m%d -d "`date` -1 day")
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop dfs -touchz viewfs://apollo-rno/apps/b_marketing_tracking/watch/${TOUCH_DT}/rotation_daily.done.${TOUCH_DT}
rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Apollo-Rno Done File error ===" | tee -a ${log_file}
    exit $rc
else
    echo `date`" =============== Apollo-Rno Done File End ===========" | tee -a ${log_file}
fi


rc=$?
if [[ $rc != 0 ]]; then
    echo " ===== rotation daily update rno hive table END With ERROR ====="  | tee -a ${log_file}
    exit $rc
else
    echo " =============== Daily Update Rotation Rno Hive Table Job End ==========="  | tee -a ${log_file}
fi