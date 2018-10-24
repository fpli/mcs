#!/bin/bash
# run job to pull rotation data from couchbase to TD
echo `date`
usage="Usage: dumpRotationToTD.sh"

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

echo `date`

DT=`date`

ROTATION_CONFIG_FILE=/chocolate/rotation/couchbase-new.properties

log_dt=$(date +%Y%m%d%H%M%S -d "$DT")
log_file="/mnt/chocolate/rotation/logs/toTD_${log_dt}.log"
echo "log_file="${log_file}
echo "DT="${DT} | tee -a ${log_file}

echo `date`" =============== Job Start ===========" | tee -a ${log_file}

java -cp /chocolate/rotation/couchbase-tool-3.3.1-SNAPSHOT-fat.jar com.ebay.traffic.chocolate.couchbase.RotationXDCR ${ROTATION_CONFIG_FILE}

rc=$?
if [[ $rc != 0 ]]; then
   echo `date`"=====================================================dumpFromCouchbase ERROR!!======================================================" | tee -a ${log_file}
   exit $rc
else
   echo "=============== dump  data from couchbase done  ==========="
   echo `date`"=====================================================dumpFromCouchbase is completed======================================================" | tee -a ${log_file}
fi


