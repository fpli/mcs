#!/bin/bash
# run job to pull rotation data from couchbase to TD
echo `date`
usage="Usage: pj_seed_job.sh [time(seconds)] [window(seconds)]"


# if no args specified, show usage
if [ $# -ne 2 ]; then
  echo $usage
  exit 1
fi

P_TIME=$1
P_WINDOW=$2

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

echo `date`


bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

DT=$(date +%Y-%m-%d -d "`date`")

SEED_CONFIG_FILE=${bin}/../conf/
OUTPUT_PATH=/mnt/chocolate/seed/data/dt=${DT}/
TIME=$(date +%s)

log_dt=$(date +%Y%m%d%H%M%S -d "$DT")
log_file="/mnt/chocolate/seed/logs/pj_log_${log_dt}.log"

echo "SEED_CONFIG_FILE="${SEED_CONFIG_FILE} | tee -a ${log_file}
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}
echo "DT="${DT} | tee -a ${log_file}
echo "TIME="${TIME} | tee -a ${log_file}
echo "log_file="${log_file}

if [ ! -d $OUTPUT_PATH ]; then
 mkdir $OUTPUT_PATH
fi

OUTPUT_PATH=${OUTPUT_PATH}"pj_data_"
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}

echo `date`" =============== Job Start ===========" | tee -a ${log_file}

java -cp ${bin}/../lib/chocolate-ingester-*.jar com.ebay.traffic.chocolate.SeedJob ${SEED_CONFIG_FILE} ${P_TIME} ${P_WINDOW} ${OUTPUT_PATH}

rc=$?
if [[ $rc != 0 ]]; then
   echo `date`"=====================================================dumpFromCouchbase ERROR!!======================================================" | tee -a ${log_file}
   exit $rc
else
   echo `date`"=====================================================dumpFromCouchbase is completed======================================================" | tee -a ${log_file}
fi


