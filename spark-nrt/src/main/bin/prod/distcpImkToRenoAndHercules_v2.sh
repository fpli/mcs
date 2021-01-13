#!/bin/bash

# Put files from chocolate hadoop to Apollo RNO and Hercules. The input files will be deleted.
# Input:    SLC Hadoop
#           /apps/tracking-events-imk/imkTransform/imkOutput
# Output:   Apollo RNO
#           /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_v2
#           Hercules
#           /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_v2/snapshot
# Schedule: /3 * ? * *

set -x

usage="Usage: distcpImkToRenoAndHercules_v2.sh"

export HADOOP_USER_NAME=chocolate
whoami
IMK_SRC_DIR="/apps/tracking-events-imk/imkTransform/imkOutput"
RENO_DEST_DIR="/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_v2"
HERCULES_DEST_DIR="/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_v2/snapshot"

echo "IMK_SRC_DIR: ${IMK_SRC_DIR}"
echo "RENO_DEST_DIR: ${RENO_DEST_DIR}"
echo "HERCULES_DEST_DIR: ${HERCULES_DEST_DIR}"

HOST_NAME=`hostname -f`
RENO_DISTCP_DIR='/datashare/mkttracking/tools/apache/distcp/apollo'
echo "HOST_NAME ：${HOST_NAME}"
echo "RENO_DISTCP_DIR ：${RENO_DISTCP_DIR}"

today=$(date +%Y-%m-%d)
yesterday=$(date --date="${today} -1days" +%Y-%m-%d)

echo "today ：${today}"
echo "yesterday ：${yesterday}"

ENV_PATH='/datashare/mkttracking/tools/cake'
JOB_NAME='DistcpImkV2ToRenoAndHerculesBatchJob'
echo "ENV_PATH:${ENV_PATH}"
echo "JOB_NAME:${JOB_NAME}"

LOCAL_PATH="/datashare/mkttracking/data/imkEtl"
if [ ! -d "${LOCAL_PATH}" ]; then
  mkdir ${LOCAL_PATH}
fi

# SLC Hadoop to Apollo RNO
cd ${RENO_DISTCP_DIR}
toyday_file=${LOCAL_PATH}/toyday_file.txt
yesterday_file=${LOCAL_PATH}/yesterday_file.txt
apollo_file=${LOCAL_PATH}/apollo_file.txt
imk_to_reno=${LOCAL_PATH}/imk_to_reno.txt

rm ${toyday_file}
rm ${yesterday_file}
rm ${apollo_file}
rm ${imk_to_reno}

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
  fi
  if [ ${orgDate} == ${yesterday} ]
  then
    echo "hdfs://slickha${one_file}" >> ${yesterday_file}
  fi
done

pwd
kinit -kt b_marketing_tracking_clients_PROD.keytab b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM
klist

function dealFileByDt() {
    dt=$1;
    echo "dt: ${dt}"
    if [ $dt == $today ]; then
        echo "Begin deal with today files: ${dt}";
        all_files_number=`cat ${toyday_file} | grep -v "^$" | wc -l`
        all_files=`cat cat ${toyday_file} | tr "\n" " "`
    elif [ $dt == $yesterday ]; then
        echo "Begin deal with yesterday files: ${dt}"
        all_files_number=`cat ${yesterday_file} | grep -v "^$" | wc -l`
        all_files=`cat cat ${yesterday_file} | tr "\n" " "`
    fi
    if [ $all_files_number -le 0 ]; then
        echo "No file need copy: ${dt}"
        return;
    fi

    RENO_DEST_FOLDER=${RENO_DEST_DIR}/dt=${dt}
    echo "RENO_DEST_FOLDER:${RENO_DEST_FOLDER}"
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p ${RENO_DEST_FOLDER}

    RENO_DEST_PATH="viewfs://apollo-rno${RENO_DEST_DIR}/dt=${dt}"
    RNO_PATH="hdfs://apollo-rno${RENO_DEST_DIR}/dt=${dt}"
    HERCULES_PATH="hdfs://hercules${HERCULES_DEST_DIR}"
    echo "RENO_DEST_PATH:${RENO_DEST_PATH}"
    echo "RNO_PATH:${RNO_PATH}"
    echo "HERCULES_PATH:${HERCULES_PATH}"

    echo "${dt} all_files:${all_files_number}"
    if [ -n "${all_files}" ]; then
    echo "distcp all_files_number"
    if [[ ${all_files_number} -gt 1 ]]; then
      hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_clients_PROD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM -targetkeytab b_marketing_tracking_clients_PROD.keytab -skipcrccheck -update ${all_files} ${RENO_DEST_PATH}
      distcp_result_code=$?
      echo "distcp_result_code:${distcp_result_code}"
      if [ ${distcp_result_code} -ne 0 ]; then
        echo "Fail to distcp from local to Apollo, please check!!!"
        exit ${distcp_result_code};
      fi
    else
      file_name=$(basename "${all_files}")
      hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_clients_PROD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM -targetkeytab b_marketing_tracking_clients_PROD.keytab -skipcrccheck -update ${all_files} ${RENO_DEST_PATH}/${file_name}
      distcp_result_code=$?
      echo "distcp_result_code:${distcp_result_code}"
      if [ ${distcp_result_code} -ne 0 ]; then
        echo "Fail to distcp from local to Apollo, please check!!!"
        exit ${distcp_result_code};
      fi
    fi
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls "${RENO_DEST_DIR}/dt=${dt}" | grep -v "^$" | awk '{print $NF}' | grep "chocolate_"  > ${apollo_file}
    # Apollo RNO to Hercules
    /datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}
    datamove_result_code=$?
    echo "datamove_result_code:${datamove_result_code}"
    if [ ${datamove_result_code} -ne 0 ]; then
      echo "Fail to datamove from Apollo to Hercules, please check!!!"
      exit ${datamove_result_code};
    fi
    for one_file in ${all_files}
    do
      file_name=$(basename "$one_file")
      echo "file_name:${file_name}"
      cnt=`cat ${apollo_file} | grep ${file_name} | wc -l`
      if [[ ${cnt} -gt 0 ]]
      then
        echo "hdfs dfs -rm ${one_file}"
        hdfs dfs -rm ${one_file}
      else
        echo "${one_file} copy fail"
        exit 1;
      fi
    done
  else
    echo "No file need to copy"
  fi
}
dealFileByDt $today;
dealFileByDt $yesterday;
echo "copy finish"
