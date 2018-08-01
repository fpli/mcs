#!/bin/bash
# run job to pull transaction data from TD to apollo
usage="Usage: dumpRotationToHadoop.sh"

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

echo `date`

DT=$(date +%Y-%m-%d -d "`date` - 1 hour")
if [ $# -eq 1 ]; then
  DT_HOUR=$(date +%Y-%m-%d' '$1:00:00 -d "`date` - 1 hour")
else
  DT_HOUR=$(date +%Y-%m-%d' '%H:00:00 -d "`date` - 1 hour")
fi
ROTATION_CONFIG_FILE=/chocolate/rotation/couchbase-dump.properties
OUTPUT_PATH=/mnt/chocolate/rotation/hive/dt=${DT}/
START_TIME=$(date +%s -d "$DT_HOUR")000
HOUR=$(date +%H -d "$DT_HOUR")

if [ ! -d ${OUTPUT_PATH} ]; then
 mkdir ${OUTPUT_PATH}
 chmod 777 ${OUTPUT_PATH}
fi
OUTPUT_PATH=/mnt/chocolate/rotation/hive/dt=${DT}/${HOUR}/
if [ ! -d ${OUTPUT_PATH} ]; then
 mkdir ${OUTPUT_PATH}
 chmod 777 ${OUTPUT_PATH}
fi

log_file="/mnt/chocolate/rotation/logs/dt=${DT}/"
if [ ! -d ${log_file} ]; then
 mkdir ${log_file}
 chmod 777 ${log_file}
fi
log_file=${log_file}${DT}_${START_TIME}.log

echo "DT="${DT} | tee -a ${log_file}
echo "DT_HOUR="${DT} | tee -a ${log_file}
echo "ROTATION_CONFIG_FILE="${ROTATION_CONFIG_FILE} | tee -a ${log_file}
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}
echo "START_TIME="${START_TIME} | tee -a ${log_file}


echo `date`" =============== Job Start ===========" | tee -a ${log_file}

java -cp /chocolate/rotation/couchbase-tool-3.2-SNAPSHOT-fat.jar com.ebay.traffic.chocolate.couchbase.DumpRotationFiles ${ROTATION_CONFIG_FILE} ${START_TIME} ${OUTPUT_PATH}
rc=$?
if [[ $rc != 0 ]]; then
   echo "=====================================================dumpRotationToHive ERROR!!======================================================" | tee -a ${log_file}
   exit $rc
else
   echo "dump  data from couchbase done"
   echo "=====================================================dumpRotationToHive is completed======================================================" | tee -a ${log_file}
fi

temp_file_name=rotation-$(date +%Y%m%d%H%M%S -d "$DT_HOUR").txt
FILE_NAME=${OUTPUT_PATH}${temp_file_name}
if [ ! -s "$FILE_NAME" ]
then exit 0
fi

kinit -kt /apache/b_marketing_tracking_APD.keytab b_marketing_tracking@APD.EBAY.COM

echo `date`"=====================================================Apollo -- LoadData started======================================================" | tee -a ${log_file}
echo `date`"------ Apollo -- LoadData started~~~" | tee -a ${log_file}
APOLLO_HDP=hdfs://apollo-phx-nn-ha/user/b_marketing_tracking/chocolate/rotation/dt=${DT}
/apache/hadoop/bin/hadoop fs -mkdir ${APOLLO_HDP}
/apache/hadoop/bin/hadoop fs -chmod 777 ${APOLLO_HDP}
APOLLO_HDP=hdfs://apollo-phx-nn-ha/user/b_marketing_tracking/chocolate/rotation/dt=${DT}/dh=${HOUR}
/apache/hadoop/bin/hadoop fs -rm -r -skipTrash ${APOLLO_HDP}
/apache/hadoop/bin/hadoop fs -mkdir ${APOLLO_HDP}
/apache/hadoop/bin/hadoop fs -chmod 777 ${APOLLO_HDP}
/apache/hadoop/bin/hadoop fs -put ${FILE_NAME} ${APOLLO_HDP}
/apache/hadoop/bin/hadoop fs -ls ${APOLLO_HDP} | tee -a ${log_file}
echo `date`"=====================================================Apollo -- LoadData Ended======================================================" | tee -a ${log_file}


echo `date`"=====================================================Ares -- LoadData Started======================================================" | tee -a ${log_file}
echo `date`"------ Ares -- LoadData started~~~" | tee -a ${log_file}
ARES_HDP=hdfs://ares-lvs-nn-ha/user/b_marketing_tracking/chocolate/rotation/dt=${DT}
/apache/hadoop_ares/bin/hadoop fs -mkdir ${ARES_HDP}
/apache/hadoop_ares/bin/hadoop fs -chmod 777 ${ARES_HDP}
ARES_HDP=hdfs://ares-lvs-nn-ha/user/b_marketing_tracking/chocolate/rotation/dt=${DT}/dh=${HOUR}
/apache/hadoop_ares/bin/hadoop fs -rm -r -skipTrash ${ARES_HDP}
/apache/hadoop_ares/bin/hadoop fs -mkdir ${ARES_HDP}
/apache/hadoop_ares/bin/hadoop fs -chmod 777 ${ARES_HDP}
/apache/hadoop_ares/bin/hadoop fs -put ${FILE_NAME} ${ARES_HDP}
/apache/hadoop_ares/bin/hadoop fs -ls ${ARES_HDP} | tee -a ${log_file}
echo `date`"=====================================================Ares -- LoadData Ended======================================================" | tee -a ${log_file}

rc=$?
if [[ $rc != 0 ]]; then
   exit $rc
else
   echo `date`" =============== Job End ===========" | tee -a ${log_file}
   exit 0
fi



