#!/bin/bash
# run job to pull transaction data from TD to apollo
usage="Usage: dumpRotationSnapshot.sh [dataStartTime]"


bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

echo `date`

DT_YMD=$(date +%Y%m%d -d "`date` - 1 hour")
DT=$(date +%Y-%m-%d -d "`date` - 1 hour")
echo $D_YM

ROTATION_CONFIG_FILE=${bin}/../conf/
OUTPUT_PATH=/datashare/mkttracking/data/rotation/snapshot/dt=${DT}/

log_file="/datashare/mkttracking/logs/rotation/snapshot/${D_YM}/"
if [ ! -d ${log_file} ]; then
 mkdir -p ${log_file}
 chmod 777 ${log_file}
fi
log_file=${log_file}"/${HOSTNAME}_"${DT_YMD}.log

echo "DT_YMD="${DT_YMD} | tee -a ${log_file}
echo "ROTATION_CONFIG_FILE="${ROTATION_CONFIG_FILE} | tee -a ${log_file}
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}

rm -r ${OUTPUT_PATH}
if [ ! -d ${OUTPUT_PATH} ]; then
 mkdir -p ${OUTPUT_PATH}
 chmod 777 ${OUTPUT_PATH}
fi

ROTATION_FILE="rotation-snapshot-"${DT}".txt"
OUTPUT_PATH=${OUTPUT_PATH}
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}

echo `date`" =============== Job Start ===========" | tee -a ${log_file}
java -cp ${bin}/../lib/couchbase-tool-*.jar com.ebay.traffic.chocolate.couchbase.DumpRotationToHadoop ${ROTATION_CONFIG_FILE} ${OUTPUT_PATH}
rc=$?
if [[ $rc != 0 ]]; then
   echo "=====================================================dumpRotationSnapshot ERROR!!======================================================" | tee -a ${log_file}
   exit $rc
else
   echo "dump  data from couchbase done"
   echo "=====================================================dumpRotationSnapshot is completed======================================================" | tee -a ${log_file}
fi

for file in ${OUTPUT_PATH}rotation* ; do
    if [[ -f $file ]]; then
        ridFiles=`wc -l ${OUTPUT_PATH}rotation*`
        ridCnt=$(awk -F' '  '{print $1}'<<<${ridFiles})
    else
        ridCnt=0
    fi
done


ridFiles=`wc -l ${OUTPUT_PATH}rotation*`
ridCnt=$(awk -F' '  '{print $1}'<<<${ridFiles})
retryCnt=0
while [ $ridCnt -le 300000 ]
do
  if [[ ${retryCnt} > 3 ]]; then
     echo "========Exceed Max RetryTimes(3) ===============" | tee -a ${log_file}
     exit 1
  fi
  echo "========dumpNumber=${retryCnt} Retry dumpRotationSnapshot ===============" | tee -a ${log_file}
  java -cp ${bin}/../lib/couchbase-tool-*.jar com.ebay.traffic.chocolate.couchbase.DumpRotationToHadoop ${ROTATION_CONFIG_FILE} ${OUTPUT_PATH}

  for file in ${OUTPUT_PATH}rotation* ; do
    if [[ -f $file ]]; then
        ridFiles=`wc -l ${OUTPUT_PATH}rotation*`
        ridCnt=$(awk -F' '  '{print $1}'<<<${ridFiles})
    else
        ridCnt=0
    fi
  done
  retryCnt=retryCnt+1
done


/datashare/mkttracking/tools/keytab-tool/kinit/kinit_byhost.sh
HDP=/apps/b_marketing_tracking/chocolate/rotation

echo `date`"=====================================================Apollo_rno -- LoadData started======================================================" | tee -a ${log_file}
echo `date`"------ Apollo_rno -- LoadData started~~~" | tee -a ${log_file}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -rm -r -skipTrash ${HDP}/dt=${DT}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -put ${OUTPUT_PATH} ${HDP}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -ls ${HDP}/dt=${DT} | tee -a ${log_file}
echo `date`"=====================================================Apollo_rno -- LoadData Ended======================================================" | tee -a ${log_file}


echo `date`"=====================================================Ares -- LoadData Started======================================================" | tee -a ${log_file}
echo `date`"------ Ares -- LoadData started~~~" | tee -a ${log_file}
/datashare/mkttracking/tools/apache/hadoop_ares/bin/hadoop fs -rm -r -skipTrash ${HDP}/dt=${DT}
/datashare/mkttracking/tools/apache/hadoop_ares/bin/hadoop fs -put ${OUTPUT_PATH} ${HDP}
/datashare/mkttracking/tools/apache/hadoop_ares/bin/hadoop fs -ls ${HDP}/dt=${DT} | tee -a ${log_file}
echo `date`"=====================================================Ares -- LoadData Ended======================================================" | tee -a ${log_file}

echo `date`" =============== Job End ===========" | tee -a ${log_file}