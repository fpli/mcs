#!/bin/bash

# Put files from chocolate hadoop to Apollo RNO and Hercules. The input files will be deleted.
# Input:    SLC Hadoop
#           /apps/tracking-events/crabTransform/imkOutput
#           /apps/tracking-events/imkTransform/imkOutput
#           /apps/tracking-events/crabTransform/dtlOutput
#           /apps/tracking-events/imkTransform/dtlOutput
# Output:   Apollo RNO
#           /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event
#           /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl
#           Hercules
#           /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event/snapshot
#           /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_dtl/snapshot
# Schedule: /3 * ? * *
# case：
#./distcpImkToRenoAndHercules.sh /apps/tracking-events/imkTransform/imkOutput /apps/tracking-events/crabTransform/imkOutput /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event/snapshot false true
#./distcpImkToRenoAndHercules.sh /apps/tracking-events/imkTransform/dtlOutput /apps/tracking-events/crabTransform/dtlOutput /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_dtl/snapshot true true

set -x

usage="Usage: distcpImkToRenoAndHercules.sh [imkSrcDir] [crabSrcDir] [renoDestDir] [herculesDestDir] [imkIsRemoveData] [crabIsRemoveData]"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

export HADOOP_USER_NAME=chocolate
whoami
IMK_SRC_DIR=$1
CRAB_SRC_DIR=$2
RENO_DEST_DIR=$3
HERCULES_DEST_DIR=$4
IMK_IS_REMOVE_DATA=$5
CRAB_IS_REMOVE_DATA=$6

TRAFFIC_MONITOR_PATH="/apps/tracking-events/imkTransform/trafficMonitor"

if [ "${IMK_IS_REMOVE_DATA}" == "true" ]
then
    IMK_IS_REMOVE_DATA="true"
fi

if [ "${CRAB_IS_REMOVE_DATA}" == "true" ]
then
    CRAB_IS_REMOVE_DATA="true"
fi

echo "IMK_SRC_DIR ：${IMK_SRC_DIR}"
echo "CRAB_SRC_DIR ：${CRAB_SRC_DIR}"
echo "RENO_DEST_DIR ：${RENO_DEST_DIR}"
echo "HERCULES_DEST_DIR ：${HERCULES_DEST_DIR}"
echo "IMK_IS_REMOVE_DATA ：${IMK_IS_REMOVE_DATA}"
echo "CRAB_IS_REMOVE_DATA ：${CRAB_IS_REMOVE_DATA}"

HOST_NAME=`hostname -f`
RENO_DISTCP_DIR='/datashare/mkttracking/tools/apache/distcp/apollo'
echo "HOST_NAME ：${HOST_NAME}"
echo "RENO_DISTCP_DIR ：${RENO_DISTCP_DIR}"
dt_hour=$(date -d '1 hour ago' +%Y-%m-%d%H)
dt=${dt_hour:0:10}
today=${dt}
yesterday=$(date --date="${today} -1days" +%Y-%m-%d)
echo "dt ：${dt}"
echo "today ：${today}"
echo "yesterday ：${yesterday}"

ENV_PATH='/datashare/mkttracking/tools/cake'
JOB_NAME='DistcpImkToRenoAndHerculesBatchJob'
echo "ENV_PATH:${ENV_PATH}"
echo "JOB_NAME:${JOB_NAME}"

if [ ! -d "~/distcp" ]; then
  mkdir ~/distcp
fi

# SLC Hadoop to Apollo RNO
cd ${RENO_DISTCP_DIR}
toyday_file=~/distcp/toyday_file.txt
yesterday_file=~/distcp/yesterday_file.txt
apollo_file=~/distcp/apollo_file.txt
crab_to_reno=~/distcp/crab_to_reno.txt
imk_to_reno=~/distcp/imk_to_reno.txt
rm ${toyday_file}
rm ${yesterday_file}
rm ${apollo_file}

hdfs dfs -ls -R ${IMK_SRC_DIR} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" > ${imk_to_reno}
all_imk_files=`cat ${imk_to_reno} | tr "\n" " "`
for one_file in ${all_imk_files}
do
  file_name=$(basename "$one_file")
  echo "file_name:${file_name}"
  orgDate=${file_name:15:10}
  if [ ${orgDate} == ${today} ]
  then
    echo "hdfs://slickha${one_file}" >> ${toyday_file}
  else
    echo "hdfs://slickha${one_file}" >> ${yesterday_file}
  fi
done

hdfs dfs -ls -R ${CRAB_SRC_DIR} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" > ${crab_to_reno}
all_crab_files=`cat ${crab_to_reno} | tr "\n" " "`
for one_file in ${all_crab_files}
do
  file_name=$(basename "$one_file")
  echo "file_name:${file_name}"
  orgDate=${file_name:15:10}
  if [ ${orgDate} == ${today} ]
  then
    echo "hdfs://slickha${one_file}" >> ${toyday_file}
  else
    echo "hdfs://slickha${one_file}" >> ${yesterday_file}
  fi
done

pwd
kinit -kt b_marketing_tracking_clients_PROD.keytab b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM
klist
RENO_DEST_FOLDER=${RENO_DEST_DIR}/dt=${today}
echo "RENO_DEST_FOLDER:${RENO_DEST_FOLDER}"
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p ${RENO_DEST_FOLDER}

RENO_DEST_PATH="viewfs://apollo-rno${RENO_DEST_DIR}/dt=${today}"
RNO_PATH="hdfs://apollo-rno${RENO_DEST_DIR}/dt=${today}"
HERCULES_PATH="hdfs://hercules${HERCULES_DEST_DIR}"
echo "RENO_DEST_PATH:${RENO_DEST_PATH}"
echo "RNO_PATH:${RNO_PATH}"
echo "HERCULES_PATH:${HERCULES_PATH}"

# SLC today files to apollo today dir
today_all_files_number=`cat ${toyday_file} | grep -v "^$" | wc -l`
if [[ ${today_all_files_number} -gt 0 ]]; then
    today_all_files=`cat cat ${toyday_file} | tr "\n" " "`
fi
echo "today_all_files:${today_all_files}"
if [ -n "${today_all_files}" ]; then
  echo "distcp today_all_files_number"
  if [[ ${today_all_files_number} -gt 1 ]]; then
    hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_clients_PROD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM -targetkeytab b_marketing_tracking_clients_PROD.keytab -skipcrccheck -update ${today_all_files} ${RENO_DEST_PATH}
  else
    file_name=$(basename "${today_all_files}")
    hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_clients_PROD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM -targetkeytab b_marketing_tracking_clients_PROD.keytab -skipcrccheck -update ${today_all_files} ${RENO_DEST_PATH}/${file_name}
  fi
  /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls "${RENO_DEST_DIR}/dt=${today}" | grep -v "^$" | awk '{print $NF}' | grep "chocolate_"  >> ${apollo_file}
  # Apollo RNO to Hercules
  /datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}
else
  echo "No file need to copy"
fi

# SLC yesterday dir to apollo yesterday dir
RENO_DEST_FOLDER=${RENO_DEST_DIR}/dt=${yesterday}
echo "RENO_DEST_FOLDER:${RENO_DEST_FOLDER}"
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p ${RENO_DEST_FOLDER}

RENO_DEST_PATH="viewfs://apollo-rno${RENO_DEST_DIR}/dt=${yesterday}"
RNO_PATH="hdfs://apollo-rno${RENO_DEST_DIR}/dt=${yesterday}"
HERCULES_PATH="hdfs://hercules${HERCULES_DEST_DIR}"
echo "RENO_DEST_PATH:${RENO_DEST_PATH}"
echo "RNO_PATH:${RNO_PATH}"

yesterday_all_files_number=`cat ${yesterday_file} | grep -v "^$" | wc -l`
if [[ ${yesterday_all_files_number} -gt 0 ]]; then
    yesterday_all_files=`cat cat ${yesterday_file} | tr "\n" " "`
fi
echo "yesterday_all_files:${yesterday_all_files}"
if [ -n "${yesterday_all_files}" ]; then
  echo "distcp yesterday_all_files"
  if [[ ${yesterday_all_files_number} -gt 1 ]]; then
    hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_clients_PROD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM -targetkeytab b_marketing_tracking_clients_PROD.keytab -skipcrccheck -update ${yesterday_all_files} ${RENO_DEST_PATH}
  else
    file_name=$(basename "${yesterday_all_files}")
    hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_clients_PROD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM -targetkeytab b_marketing_tracking_clients_PROD.keytab -skipcrccheck -update ${yesterday_all_files} ${RENO_DEST_PATH}/${file_name}
  fi
  /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls "${RENO_DEST_DIR}/dt=${yesterday}" | grep -v "^$" | awk '{print $NF}' | grep "chocolate_"  >> ${apollo_file}
  # Apollo RNO to Hercules
  /datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}
else
  echo "No file need to copy"
fi

success_file_numbers=0;
fail_file_numbers=0;
for one_file in ${all_imk_files}
do
  file_name=$(basename "$one_file")
  echo "file_name:${file_name}"
  cnt=`cat ${apollo_file} | grep ${file_name} | wc -l`
  if [[ ${cnt} -gt 0 ]]
  then
      success_file_numbers=$[success_file_numbers+1];
      if [ "${IMK_IS_REMOVE_DATA}" == "true" ]
      then
        echo "hdfs dfs -rm ${one_file}"
        hdfs dfs -rm ${one_file}
      else
        echo "hdfs dfs -mv ${one_file} ${TRAFFIC_MONITOR_PATH}"
        hdfs dfs -mv ${one_file} ${TRAFFIC_MONITOR_PATH}
      fi
  else
    fail_file_numbers=$[fail_file_numbers+1];
    echo "${one_file} copy fail"
  fi
done
echo "imkTransform success copy file size:${success_file_numbers}"
echo "imkTransform fail copy file size:${fail_file_numbers}"

success_file_numbers=0;
fail_file_numbers=0;
for one_file in ${all_crab_files}
do
  file_name=$(basename "$one_file")
  echo "file_name:${file_name}"
  cnt=`cat ${apollo_file} | grep ${file_name} | wc -l`
  if [[ ${cnt} -gt 0 ]]
  then
      success_file_numbers=$[success_file_numbers+1];
      if [ "${CRAB_IS_REMOVE_DATA}" == "true" ]
      then
        echo "hdfs dfs -rm ${one_file}"
        hdfs dfs -rm ${one_file}

      else
        echo "hdfs dfs -mv ${one_file} ${TRAFFIC_MONITOR_PATH}"
        hdfs dfs -mv ${one_file} ${TRAFFIC_MONITOR_PATH}
      fi
  else
    fail_file_numbers=$[fail_file_numbers+1];
    echo "${one_file} copy fail"
  fi
done
echo "crabTransform success copy file size:${success_file_numbers}"
echo "crabTransform fail copy file size:${fail_file_numbers}"
echo "copy finish"
