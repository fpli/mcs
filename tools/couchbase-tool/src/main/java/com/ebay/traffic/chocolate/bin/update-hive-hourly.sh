#!/bin/bash
# run job to pull transaction data from TD to apollo
usage="Usage: dumpRotationToHive.sh [batchDate]"

# if no args specified, show usage
#if [ $# -le 0 ]; then
#  echo $usage
#  exit 1
#fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

echo `date`

DT=$(date +%Y-%m-%d -d "`date` -2 hour")
if [ $# -eq 1 ]; then
  DT_HOUR=$(date +%Y-%m-%d' '$1:00:00 -d "`date` - 2 hour")
else
  DT_HOUR=$(date +%Y-%m-%d' '%H:00:00 -d "`date` - 2 hour")
fi
JOB_TIME=$(date +%Y%m%d%H%M%S -d "`date`")
HOUR=$(date +%H -d "$DT_HOUR")

SQL_FILE=/home/yimeng/rotation/sql/choco_rotation_update.sql
#HDP=hdfs://apollo-phx-nn-ha/user/b_marketing_tracking/chocolate/rotation/dt=${DT}/dh=${HOUR}
HDP=hdfs://ares-lvs-nn-ha/user/b_marketing_tracking/chocolate/rotation/dt=${DT}/dh=${HOUR}

log_file="/home/yimeng/rotation/logs/dt=${DT}/"
if [ ! -d ${log_file} ]; then
 mkdir ${log_file}
 chmod 777 ${log_file}
fi
log_file=${log_file}${DT}_${JOB_TIME}.log

echo "DT="${DT} | tee -a ${log_file}
echo "DT_HOUR="${DT} | tee -a ${log_file}
echo "JOB_TIME="${JOB_TIME} | tee -a ${log_file}
echo "SQL_FILE="${SQL_FILE} | tee -a ${log_file}
echo "HDP="${HDP} | tee -a ${log_file}




echo `date`" =============== Job Start ===========" | tee -a ${log_file}

kinit -kt /apache/b_marketing_tracking_APD.keytab b_marketing_tracking@APD.EBAY.COM

echo `date`"-------- LoadData started~~~" | tee -a ${log_file}

/apache/hadoop/bin/hadoop fs -ls ${HDP} | tee -a ${log_file}
/apache/hadoop/bin/hadoop fs -ls ${HDP}
rc=$?
if [[ $rc != 0 ]]; then
   echo "No rotation info need to be updated to HIVE." | tee -a ${log_file}
   exit
else
   echo "Some rotation info need to be updated to HIVE." | tee -a ${log_file}
fi

AHDP_PATH=${HDP}
sqlParam="==rotation_hdp=="
echo `date`"------ set Apollo hdfs file path into hive sql: "${HDP} | tee -a ${log_file}
AHDP_PATH=${AHDP_PATH//\//\\/}
sed -i -E "s/${sqlParam}/${AHDP_PATH}/g" ${SQL_FILE}  | tee -a ${log_file}
cat ${SQL_FILE} | grep 'LOCATION' | tee -a ${log_file}

/apache/hive/bin/hive -f ${SQL_FILE} | tee -a ${log_file}

echo `date`"------ rollback sql parameter in hive sql: "${sqlParam} | tee -a ${log_file}
sed -i -E "s/${AHDP_PATH}/${sqlParam}/g" ${SQL_FILE}
cat ${SQL_FILE} | grep 'LOCATION' | tee -a ${log_file}

echo `date`" =============== Job End ===========" | tee -a ${log_file}


