#!/bin/bash
# run job to pull transaction data from TD to apollo
usage="Usage: dumpRotationSnapshot.sh [dataStartTime]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

dataStartTime=$1

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

echo `date`

DT=$(date +%Y-%m-%d -d "`date`")
ROTATION_CONFIG_FILE=${bin}/../conf/
OUTPUT_PATH=/mnt/chocolate/rotation/snapshot/dt=${DT}/

log_file="/mnt/chocolate/rotation/logs/dt=${DT}/"
if [ ! -d ${log_file} ]; then
 mkdir ${log_file}
 chmod 777 ${log_file}
fi
log_file=${log_file}${DT}.log

echo "DT="${DT} | tee -a ${log_file}
echo "ROTATION_CONFIG_FILE="${ROTATION_CONFIG_FILE} | tee -a ${log_file}
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}

rm -r ${OUTPUT_PATH}
if [ ! -d ${OUTPUT_PATH} ]; then
 mkdir -p ${OUTPUT_PATH}
 chmod 777 ${OUTPUT_PATH}
fi

echo `date`" =============== Job Start ===========" | tee -a ${log_file}
java -cp ${bin}/../lib/couchbase-tool-*.jar com.ebay.traffic.chocolate.couchbase.DumpRotationSnapshot ${ROTATION_CONFIG_FILE} ${OUTPUT_PATH} ${dataStartTime}
rc=$?
if [[ $rc != 0 ]]; then
   echo "=====================================================dumpRotationSnapshot ERROR!!======================================================" | tee -a ${log_file}
   exit $rc
else
   echo "dump  data from couchbase done"
   echo "=====================================================dumpRotationSnapshot is completed======================================================" | tee -a ${log_file}
fi

kinit -kt /apache/b_marketing_tracking_APD.keytab b_marketing_tracking@APD.EBAY.COM

echo `date`"=====================================================Apollo -- LoadData started======================================================" | tee -a ${log_file}
echo `date`"------ Apollo -- LoadData started~~~" | tee -a ${log_file}
APOLLO_HDP=hdfs://apollo-phx-nn-ha/user/b_marketing_tracking/chocolate/rotation-snapshot
/apache/hadoop/bin/hadoop fs -rm -r -skipTrash ${APOLLO_HDP}/dt=${DT}
/apache/hadoop/bin/hadoop fs -put ${OUTPUT_PATH} ${APOLLO_HDP}
/apache/hadoop/bin/hadoop fs -ls ${APOLLO_HDP}/dt=${DT} | tee -a ${log_file}
echo `date`"=====================================================Apollo -- LoadData Ended======================================================" | tee -a ${log_file}


echo `date`"=====================================================Ares -- LoadData Started======================================================" | tee -a ${log_file}
echo `date`"------ Ares -- LoadData started~~~" | tee -a ${log_file}
ARES_HDP=hdfs://ares-lvs-nn-ha/user/b_marketing_tracking/chocolate/rotation
/apache/hadoop_ares/bin/hadoop fs -rm -r -skipTrash ${ARES_HDP}/dt=${DT}
/apache/hadoop_ares/bin/hadoop fs -put -r ${OUTPUT_PATH} ${ARES_HDP}
/apache/hadoop_ares/bin/hadoop fs -ls ${ARES_HDP}/dt=${DT} | tee -a ${log_file}
echo `date`"=====================================================Ares -- LoadData Ended======================================================" | tee -a ${log_file}

echo `date`" =============== Job End ===========" | tee -a ${log_file}