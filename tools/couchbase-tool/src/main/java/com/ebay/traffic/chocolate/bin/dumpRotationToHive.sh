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

DT=$(date +%Y-%m-%d -d "`date` - 1 day")
#DT=$(date +%Y-%m-%d' '%H:00:00 -d "`date` - 1 hour")
ROTATION_CONFIG_FILE=/chocolate/rotation/couchbase.properties
OUTPUT_PATH=/mnt/chocolate/rotation/hive/dt=${DT}
START_TIME=$(date +%s -d "$DT 00:00:00")000

echo "DT="${DT}
echo "ROTATION_CONFIG_FILE="${ROTATION_CONFIG_FILE}
echo "OUTPUT_PATH="${OUTPUT_PATH}
echo "START_TIME="${START_TIME}

log_file="/mnt/chocolate/rotation/logs/$DT_$START_TIME.log"
echo `date`" =============== Job Start ===========" > ${log_file}

java -cp /chocolate/rotation/couchbase-tool-3.0-RELEASE-fat.jar com.ebay.traffic.chocolate.couchbase.DumpRotationFiles ${ROTATION_CONFIG_FILE} ${START_TIME} ${OUTPUT_PATH}
rc=$?
if [[ $rc != 0 ]]; then
   echo "=====================================================dumpRotationToHive ERROR!!======================================================" >> ${log_file}
   exit $rc
else
   echo "dump  data from couchbase done"
   echo "=====================================================dumpRotationToHive is completed======================================================" >> ${log_file}
fi

kinit -kt /apache/b_marketing_tracking_APD.keytab b_marketing_tracking@APD.EBAY.COM

echo "------ Apollo -- LoadData started~~~"
APOLLO_HDP=hdfs://apollo-phx-nn-ha/user/b_marketing_tracking/chocolate/rotation/dt=${DT}
/apache/hadoop/bin/hadoop fs -rm -r -skipTrash ${APOLLO_HDP}
/apache/hadoop/bin/hadoop fs -mkdir ${APOLLO_HDP}
/apache/hadoop/bin/hadoop fs -chmod 777 ${APOLLO_HDP}
/apache/hadoop/bin/hadoop fs -put ${OUTPUT_PATH}/* ${APOLLO_HDP}

/apache/hive_apollo/bin/hive -f /chocolate/rotation/choco_rotation.sql


echo "------ Ares -- LoadData started~~~"
ARES_HDP=hdfs://ares-lvs-nn-ha/user/b_marketing_tracking/chocolate/rotation/dt=${DT}
/apache/hadoop_ares/bin/hadoop fs -rm -r -skipTrash ${ARES_HDP}
/apache/hadoop_ares/bin/hadoop fs -mkdir ${ARES_HDP}
/apache/hadoop_ares/bin/hadoop fs -chmod 777 ${ARES_HDP}
/apache/hadoop_ares/bin/hadoop fs -put ${OUTPUT_PATH}/* ${ARES_HDP}

/apache/hive_ares/bin/hive -f /chocolate/rotation/choco_rotation.sql

echo `date`" =============== Job End ===========" > ${log_file}

