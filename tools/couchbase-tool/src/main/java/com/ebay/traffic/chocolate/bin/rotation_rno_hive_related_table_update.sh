#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

DS=$1
log_file=$2

SQL_FILE=/datashare/mkttracking/jobs/tdmoveoff/rotation/sql/rno/dw_mpx_${DS}_ups.sql

echo "SQL_FILE="${SQL_FILE} | tee -a ${log_file}
echo "DS="${DS} | tee -a ${log_file}

echo `date`" =============== Job Start ===========" | tee -a ${log_file}
echo `date`"-------- Apollo-rno LoadData ${DS} started~~~" | tee -a ${log_file}

/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -f ${SQL_FILE}

rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Apollo-Rno ${DS} Table Job error ===" | tee -a ${log_file}
    exit $rc
else
    echo `date`" =============== Apollo-Rno ${DS} Table Job End ===========" | tee -a ${log_file}
fi