#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

DS=$1
log_file=$2

SQL_FILE=/datashare/mkttracking/jobs/tdmoveoff/rotation/sql/hercules/dw_mpx_${DS}_ups.sql

echo "SQL_FILE="${SQL_FILE} | tee -a ${log_file}
echo "DS="${DS} | tee -a ${log_file}

echo `date`" =============== Job Start ===========" | tee -a ${log_file}
echo `date`"-------- Hercules LoadData ${DS} started~~~" | tee -a ${log_file}

/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -f ${SQL_FILE}

rc=$?
if [[ $rc != 0 ]]; then
    echo `date`"===  Hercules ${DS} Table Job error ==="
    exit $rc
else
    echo `date`" =============== Hercules ${DS} Table Job End ==========="
fi