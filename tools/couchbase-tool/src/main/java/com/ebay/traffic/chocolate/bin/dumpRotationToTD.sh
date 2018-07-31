#!/bin/bash
# run job to pull rotation data from couchbase to TD
echo `date`
usage="Usage: dumpRotationToTD.sh"

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

echo `date`

DT=$(date +%Y-%m-%d -d "`date`")
if [ $# -eq 1 ]; then
  DT_HOUR=$(date +%Y-%m-%d' '$1:00:00 -d "`date` - 2 hour")
else
  DT_HOUR=$(date +%Y-%m-%d' '%H:00:00 -d "`date` - 2 hour")
fi

ROTATION_CONFIG_FILE=/chocolate/rotation/couchbase.properties
OUTPUT_PATH=/mnt/chocolate/rotation/teradata/dt=${DT}/
JOB_DATA_TIME=$(date +%Y-%m-%d' '23:00:00 -d "`date` - 1 day")
START_TIME=$(date +%s -d "$JOB_DATA_TIME")000
HOUR=$(date +%H -d "$DT_HOUR")

log_dt=$(date +%Y%m%d%H%M%S -d "$DT_HOUR")
log_file="/mnt/chocolate/rotation/logs/toTD_${log_dt}.log"
echo "log_file="${log_file}
echo "DT="${DT} | tee -a ${log_file}
echo "DT_HOUR"=${DT_HOUR} | tee -a ${log_file}
echo "ROTATION_CONFIG_FILE="${ROTATION_CONFIG_FILE} | tee -a ${log_file}
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}
echo "START_TIME="${START_TIME} | tee -a ${log_file}
echo "HOUR="${HOUR} | tee -a ${log_file}

if [ ! -d $OUTPUT_PATH ]; then
 mkdir $OUTPUT_PATH
fi

DT_HOUR_FORMAT=$(date +%Y-%m-%d_%H_ -d "$DT_HOUR")
OUTPUT_PATH=${OUTPUT_PATH}${DT_HOUR_FORMAT}
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}

echo `date`" =============== Job Start ===========" | tee -a ${log_file}


if [[ $HOUR -eq 23 ]]; then
   echo `date`" =============== dump rotation files from couchbase by the date $DT_HOUR===========" | tee -a ${log_file}
   java -cp /chocolate/rotation/couchbase-tool-3.1-RELEASE-fat.jar com.ebay.traffic.chocolate.couchbase.DumpLegacyRotationFiles ${ROTATION_CONFIG_FILE} ${START_TIME} ${OUTPUT_PATH}
else
   echo `date`"=============== dump empty files ===========" | tee -a ${log_file}
   java -cp /chocolate/rotation/couchbase-tool-3.1-RELEASE-fat.jar com.ebay.traffic.chocolate.couchbase.DumpLegacyRotationFiles ${ROTATION_CONFIG_FILE} "" ${OUTPUT_PATH}
fi

rc=$?
if [[ $rc != 0 ]]; then
   echo `date`"=====================================================dumpFromCouchbase ERROR!!======================================================" | tee -a ${log_file}
   exit $rc
else
   echo "=============== dump  data from couchbase done  ==========="
   echo `date`"=====================================================dumpFromCouchbase is completed======================================================" | tee -a ${log_file}
fi


