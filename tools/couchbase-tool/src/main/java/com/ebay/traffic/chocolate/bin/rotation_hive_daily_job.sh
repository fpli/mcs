#!/bin/bash
# run job to pull transaction data from TD to apollo
usage="Usage: dumpRotationToHive.sh [batchDate] [HDP]"

# if no args specified, show usage
#if [ $# -le 0 ]; then
#  echo $usage
#  exit 1
#fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`


DT=$(date +%Y-%m-%d -d "`date` -1 hour")
SQL_FILE=/datashare/mkttracking/jobs/rotation/sql/choco_rotation.sql
HDP=/apps/b_marketing_tracking/chocolate/rotation/dt=${DT}

HOST_NAME=`hostname -f`

log_file="/datashare/mkttracking/logs/rotation/hive/dt=${DT}/"
if [ ! -d ${log_file} ]; then
 mkdir ${log_file}
 chmod 777 ${log_file}
fi
log_file=${log_file}daily_${HOST_NAME}_$(date +%s).log

echo "DT="${DT} | tee -a ${log_file}
echo "SQL_FILE="${SQL_FILE} | tee -a ${log_file}
echo "HDP="${HDP} | tee -a ${log_file}
echo "LOG_FILE"${log_file}

echo `date`" =============== Job Start ===========" | tee -a ${log_file}
/datashare/mkttracking/tools/keytab-tool/kinit/kinit_byhost.sh

echo `date`"-------- LoadData started~~~" | tee -a ${log_file}

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -ls ${HDP} | tee -a ${log_file}
ridCnt=`/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -cat ${HDP}/rotation* |wc -l`

if [[ ${ridCnt} > 300000 ]]; then
    sqlParam="==rotation_hdp=="

    echo `date`"------ set Apollo hdfs file path into hive sql: "${HDP} | tee -a ${log_file}
    HDP=${HDP//\//\\/}
    sed -i -E "s/${sqlParam}/${HDP}/g" ${SQL_FILE}  | tee -a ${log_file}
    cat ${SQL_FILE} | grep 'LOCATION' | tee -a ${log_file}
    /datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -f ${SQL_FILE} | tee -a ${log_file}

    echo `date`"------ rollback sql parameter in hive sql: "${sqlParam} | tee -a ${log_file}
    sed -i -E "s/${HDP}/${sqlParam}/g" ${SQL_FILE}
    cat ${SQL_FILE} | grep 'LOCATION' | tee -a ${log_file}
    echo `date`" =============== Job End ===========" | tee -a ${log_file}
else
   exit 1
fi



