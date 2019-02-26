#!/bin/bash
# run job to pull rotation data from couchbase to TD
echo `date`
usage="Usage: dumpRotationToTD.sh"

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

DT=$(date +%Y-%m-%d -d "`date` - 1 hour")
if [ $# -eq 1 ]; then
  DT_HOUR=$(date +%Y-%m-%d' '$1:00:00 -d "`date` - 1 hour")
else
  DT_HOUR=$(date +%Y-%m-%d' '%H:00:00 -d "`date` - 1 hour")
fi

ROTATION_CONFIG_FILE=${bin}/../conf/
OUTPUT_PATH=/datashare/mkttracking/data/rotation/teradata/dt=${DT}/

HOUR=$(date +%H -d "$DT_HOUR")

if [[ $HOUR -eq 23 ]]; then
    JOB_DATA_TIME=$(date +%Y-%m-%d' '23:00:00 -d "$DT - 1 day")
else
    JOB_DATA_TIME=${DT_HOUR}
fi
START_TIME=$(date +%s -d "$JOB_DATA_TIME")000
END_TIME=$(date +%s)000

log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S -d "$DT_HOUR")
log_file="/datashare/mkttracking/logs/rotation/teradata/${log_dt}.log"
echo "log_file="${log_file}
echo "DT="${DT} | tee -a ${log_file}
echo "JOB_DATA_TIME="${JOB_DATA_TIME} | tee -a ${log_file}
echo "DT_HOUR"=${DT_HOUR} | tee -a ${log_file}
echo "ROTATION_CONFIG_FILE="${ROTATION_CONFIG_FILE} | tee -a ${log_file}
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}
echo "START_TIME="${START_TIME} | tee -a ${log_file}
echo "END_TIME="${END_TIME} | tee -a ${log_file}
echo "HOUR="${HOUR} | tee -a ${log_file}

if [ ! -d $OUTPUT_PATH ]; then
 mkdir $OUTPUT_PATH
fi

DT_HOUR_FORMAT=$(date +%Y-%m-%d_%H_ -d "$DT_HOUR")
OUTPUT_PATH=${OUTPUT_PATH}${DT_HOUR_FORMAT}
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}

echo `date`" =============== Job Start ===========" | tee -a ${log_file}


echo `date`" =============== dump rotation files from couchbase by the date $DT_HOUR===========" | tee -a ${log_file}
java -cp ${bin}/../lib/couchbase-tool-*.jar com.ebay.traffic.chocolate.couchbase.DumpRotationToTD ${ROTATION_CONFIG_FILE} ${START_TIME} ${END_TIME} ${OUTPUT_PATH}

rc=$?
if [[ ${rc} != 0 ]]; then
   echo `date`"=====================================================dumpFromCouchbase ERROR!!======================================================" | tee -a ${log_file}
   exit ${rc}
else
   echo "=============== dump  data from couchbase done  ==========="
   echo `date`"=====================================================dumpFromCouchbase is completed======================================================" | tee -a ${log_file}
fi