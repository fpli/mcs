#!/bin/bash

# Put files from chocolate hadoop to Apollo RNO and Hercules. The input files will be deleted.
# Input:    lvs Hadoop
#           /apps/tracking-events-workdir/meta/EPN/output/epnnrt_scp_click
#           hdfs://elvisha/apps/epn-nrt/click/date=
# Output:   Apollo RNO
#           /apps/b_marketing_tracking/chocolate/epnnrt/click/click_dt=
#           Hercules
#           /sys/edw/imk/im_tracking/epn/ams_click/snapshot/click_dt=
# Input:    lvs Hadoop
#           /apps/tracking-events-workdirmeta/EPN/output/epnnrt_scp_imp
#           hdfs://elvisha/apps/epn-nrt/impression/date=
# Output:   Apollo RNO
#           /apps/b_marketing_tracking/chocolate/epnnrt/imp/imprsn_dt=
#           Hercules
#           /sys/edw/imk/im_tracking/epn/ams_impression/snapshot/imprsn_dt=
# Schedule: /3 * ? * *
# case：
#./distcpAmsToRenoAndHercules.sh /apps/epn-nrt/click /apps/b_marketing_tracking/chocolate/epnnrt/click /sys/edw/imk/im_tracking/epn/ams_click/snapshot click
#./distcpAmsToRenoAndHercules.sh /apps/epn-nrt/impression /apps/b_marketing_tracking/chocolate/epnnrt/imp /sys/edw/imk/im_tracking/epn/ams_impression/snapshot imp

set -x

usage="Usage: distcpAmsToRenoAndHercules.sh [metaDir] [renoDestDir] [herculesDestDir] [type]"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

export HADOOP_USER_NAME=chocolate
whoami
META_DIR=$1
RENO_DEST_DIR=$2
HERCULES_DEST_DIR=$3
TYPE=$4

if [ ${TYPE} == "click" ]
then
  DEST_DIR_PREFIX="click_dt"
elif [ ${TYPE} == "imp"  ]; then
  DEST_DIR_PREFIX="imprsn_dt"
else
  echo $usage
  exit 1
fi

echo "META_DIR:${META_DIR}"
echo "RENO_DEST_DIR:${RENO_DEST_DIR}"
echo "HERCULES_DEST_DIR:${HERCULES_DEST_DIR}"

HOST_NAME=`hostname -f`
RENO_DISTCP_DIR='/datashare/mkttracking/tools/apache/distcp/apollo'
LOCAL_TMP_DIR='/datashare/mkttracking/data/epn-nrt/distcpTmp'
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
JOB_NAME='DistcpAmsToRenoAndHerculesBatchJob'
echo "ENV_PATH:${ENV_PATH}"
echo "JOB_NAME:${JOB_NAME}"

cd ${RENO_DISTCP_DIR}
apollo_file=${LOCAL_TMP_DIR}/apollo_file.txt
hercules_file=${LOCAL_TMP_DIR}/hercules_file.txt
rm ${apollo_file}
rm ${hercules_file}

pwd
kinit -kt b_marketing_tracking_clients_PROD.keytab b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM
klist

META_PATH="hdfs://elvisha${META_DIR}/date=${yesterday}"
RENO_DEST_PATH="viewfs://apollo-rno${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday}"
RNO_PATH="hdfs://apollo-rno${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday}"
HERCULES_PATH="hdfs://hercules${HERCULES_DEST_DIR}"
meta_yesterday_files_nums=`hdfs dfs -ls ${META_PATH} | grep dw_ams | wc -l`
reno_yesterday_files_nums=`/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls ${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday} | grep dw_ams | wc -l`

if [ ${meta_yesterday_files_nums} == ${reno_yesterday_files_nums} -a ${meta_yesterday_files_nums} -gt 0 ]
then
  echo "${yesterday} files had copied"
fi

if [ ${meta_yesterday_files_nums} -lt ${reno_yesterday_files_nums} ]
then
  echo "${yesterday} meta_yesterday_files_nums(${meta_yesterday_files_nums}) less than reno_yesterday_files_nums(${reno_yesterday_files_nums})"
fi

if [ ${meta_yesterday_files_nums} -gt ${reno_yesterday_files_nums} ]
then
  /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p "${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday}"
  hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_clients_PROD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM -targetkeytab b_marketing_tracking_clients_PROD.keytab -skipcrccheck -update ${META_PATH} ${RENO_DEST_PATH}
  # Apollo RNO to Hercules
  /datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}

  /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls "${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday}" | grep -v "^$" | awk '{print $NF}' | grep dw_ams > ${apollo_file}
  /datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -ls "${HERCULES_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday}" | grep -v "^$" | awk '{print $NF}' | grep dw_ams > ${hercules_file}
fi

META_PATH="hdfs://elvisha${META_DIR}/date=${today}"
RENO_DEST_PATH="viewfs://apollo-rno${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${today}"
RNO_PATH="hdfs://apollo-rno${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${today}"
HERCULES_PATH="hdfs://hercules${HERCULES_DEST_DIR}"
meta_today_files_nums=`hdfs dfs -ls ${META_PATH} | grep dw_ams | wc -l`
reno_today_files_nums=`/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls ${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${today} |  grep dw_ams | wc -l`

if [ ${meta_today_files_nums} == ${reno_today_files_nums} -a ${meta_today_files_nums} -gt 0 ]
then
  echo "${today} files had copied"
fi

if [ ${meta_today_files_nums} -lt ${reno_today_files_nums} ]
then
  echo "${today} meta_today_files_nums(${meta_today_files_nums}) less than reno_today_files_nums(${reno_today_files_nums})"
fi

if [ ${meta_today_files_nums} -gt ${reno_today_files_nums} ]
then
  /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p "${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${today}"
  hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_clients_PROD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM -targetkeytab b_marketing_tracking_clients_PROD.keytab -skipcrccheck -update ${META_PATH} ${RENO_DEST_PATH}
  # Apollo RNO to Hercules
  /datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}

  /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls "${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${today}" | grep -v "^$" | awk '{print $NF}' | grep dw_ams >> ${apollo_file}
  /datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -ls "${HERCULES_DEST_DIR}/${DEST_DIR_PREFIX}=${today}" | grep -v "^$" | awk '{print $NF}' | grep dw_ams >> ${hercules_file}
fi

# deal metaFile
function process_meta_file(){
  all_meta_files=$1
  cluster_file=$2
  all_files=`cat ${all_meta_files} | tr "\n" " "`
  for one_meta in ${all_files}
  do
      output_file="${LOCAL_TMP_DIR}/output_file.txt"
      meta_file_name=$(basename "${one_meta}")
      rm -f ${meta_file_name}
      hdfs dfs -get ${one_meta}
      python /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/readMetaFile.py ${meta_file_name} ${output_file}
      rcode=$?
      if [ ${rcode} -ne 0 ]
      then
          echo -e "Failed to parse meta file: ${meta_file_name}!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in parsing meta file!!!" -v DL-eBay-Chocolate-GC@ebay.com
          exit ${rcode}
      fi
      data_files=`cat ${output_file} | grep -v "^$" | awk '{print $NF}' | grep dw_ams`
      files_size=`cat ${output_file} | wc -l`
      echo "${meta_file_name} size:"${files_size}
      had_copied_number=0;
      for one_file in ${data_files}
      do
        file_name=$(basename "${one_file}")
        cnt=`cat ${cluster_file} | grep ${file_name} | wc -l`
        if [[ ${cnt} -gt 0 ]]
        then
          had_copied_number=$[had_copied_number+1];
        else
          echo "${one_file} not uploaded yet"
          break
        fi
      done
      if [ ${files_size} == ${had_copied_number} ]
      then
        hdfs dfs -rm ${one_meta}
        echo "${one_meta} can delete"
      else
        echo "${one_meta} can't delete"
      fi
      rm ${meta_file_name}
  done
}

cd /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod

./putAmsHourlyDoneToRenoAndHercules.sh ${TYPE}

cd ${LOCAL_TMP_DIR}

all_meta_files="${LOCAL_TMP_DIR}/all_meta_files.txt"
epn_output_path="hdfs://elvisha/apps/tracking-events-workdir/meta/EPN/output"
#epn_output_path="hdfs://elvisha/apps/tracking-events/test/output"
if [ "${TYPE}" == "click" ]
then
  hdfs dfs -ls ${epn_output_path}/epnnrt_scp_click  | grep .epnnrt_reno  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: > ${all_meta_files}
  process_meta_file ${all_meta_files} ${apollo_file}
  hdfs dfs -ls ${epn_output_path}/epnnrt_scp_click  | grep .epnnrt_hercules  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: > ${all_meta_files}
  process_meta_file ${all_meta_files} ${hercules_file}
elif [ "${TYPE}" == "imp" ]
then
    hdfs dfs -ls ${epn_output_path}/epnnrt_scp_imp  | grep .epnnrt_reno  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: > ${all_meta_files}
    process_meta_file ${all_meta_files} ${apollo_file}
    hdfs dfs -ls ${epn_output_path}/epnnrt_scp_imp  | grep .epnnrt_hercules  |  grep -v "^$" | awk '{print $NF}' | grep hdfs:> ${all_meta_files}
    process_meta_file ${all_meta_files} ${hercules_file}
else
    echo "Wrong type to metaFile!"
    exit 1
fi

echo "distcpAmsToRenoAndHercules copy finish"

