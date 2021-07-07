#!/bin/bash
set -x

usage="Usage: distcpAmsRenoToHercules_v3.sh [metaDir] [renoDestDir] [herculesDestDir] [type]"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi
cd /mnt/jobs/tracking/epn-nrt/bin
export HADOOP_USER_NAME=chocolate
whoami
RENO_DEST_DIR=$1
HERCULES_DEST_DIR=$2
TYPE=$3

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
LOCAL_TMP_DIR='/datashare/mkttracking/data/epn-nrt-v2/distcpTmp'
echo "HOST_NAME ：${HOST_NAME}"
today=$(date +%Y-%m-%d)
yesterday=$(date --date="${today} -1days" +%Y-%m-%d)
echo "today ：${today}"
echo "yesterday ：${yesterday}"

ENV_PATH='/datashare/mkttracking/tools/cake'
JOB_NAME='DistcpAmsToRenoAndHerculesBatchJob_v3'
echo "ENV_PATH:${ENV_PATH}"
echo "JOB_NAME:${JOB_NAME}"

apollo_file=${LOCAL_TMP_DIR}/apollo_file.txt
hercules_file=${LOCAL_TMP_DIR}/hercules_file.txt
rm ${apollo_file}
rm ${hercules_file}

apollo_command=/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs
hercules_command=/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs
RNO_DEST_PATH="viewfs://apollo-rno${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday}"
RNO_PATH="hdfs://apollo-rno${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday}"
HERCULES_PATH="hdfs://hercules${HERCULES_DEST_DIR}"
meta_yesterday_files_nums=`${apollo_command} dfs -ls ${RNO_DEST_PATH} | grep dw_ams | wc -l`
hercules_yesterday_files_nums=`${hercules_command} dfs -ls "${HERCULES_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday}" |  grep dw_ams | wc -l`

if [ ${meta_yesterday_files_nums} == ${hercules_yesterday_files_nums} -a ${meta_yesterday_files_nums} -gt 0 ]
then
  echo "${yesterday} files had copied"
fi

if [ ${meta_yesterday_files_nums} -lt ${hercules_yesterday_files_nums} ]
then
  echo "${yesterday} meta_yesterday_files_nums(${meta_yesterday_files_nums}) less than hercules_yesterday_files_nums(${hercules_yesterday_files_nums})"
fi

if [ ${meta_yesterday_files_nums} -gt ${hercules_yesterday_files_nums} ]
then
  # Apollo RNO to Hercules
  /datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}
  datamove_result_code=$?
  echo "datamove_result_code:${datamove_result_code}"
  if [ ${datamove_result_code} -ne 0 ]; then
    echo "Fail to datamove from Apollo to Hercules, please check!!!"
    exit ${datamove_result_code};
  fi
fi

${apollo_command} dfs -ls "${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday}" | grep -v "^$" | awk '{print $NF}' | grep dw_ams > ${apollo_file}
${hercules_command} dfs -ls "${HERCULES_DEST_DIR}/${DEST_DIR_PREFIX}=${yesterday}" | grep -v "^$" | awk '{print $NF}' | grep dw_ams > ${hercules_file}

RNO_DEST_PATH="viewfs://apollo-rno${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${today}"
RNO_PATH="hdfs://apollo-rno${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${today}"
HERCULES_PATH="hdfs://hercules${HERCULES_DEST_DIR}"
meta_today_files_nums=`${apollo_command} dfs -ls ${RNO_DEST_PATH} | grep dw_ams | wc -l`
hercules_today_files_nums=`${hercules_command} dfs -ls "${HERCULES_DEST_DIR}/${DEST_DIR_PREFIX}=${today}" |  grep dw_ams | wc -l`

if [ ${meta_today_files_nums} == ${hercules_today_files_nums} -a ${meta_today_files_nums} -gt 0 ]
then
  echo "${today} files had copied"
fi

if [ ${meta_today_files_nums} -lt ${hercules_today_files_nums} ]
then
  echo "${today} meta_today_files_nums(${meta_today_files_nums}) less than hercules_today_files_nums(${hercules_today_files_nums})"
fi

if [ ${meta_today_files_nums} -gt ${hercules_today_files_nums} ]
then
  # Apollo RNO to Hercules
  /datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}
  datamove_result_code=$?
  echo "datamove_result_code:${datamove_result_code}"
  if [ ${datamove_result_code} -ne 0 ]; then
    echo "Fail to datamove from Apollo to Hercules, please check!!!"
    exit ${datamove_result_code};
  fi
fi

${apollo_command} dfs -ls "${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${today}" | grep -v "^$" | awk '{print $NF}' | grep dw_ams >> ${apollo_file}
${hercules_command} dfs -ls "${HERCULES_DEST_DIR}/${DEST_DIR_PREFIX}=${today}" | grep -v "^$" | awk '{print $NF}' | grep dw_ams >> ${hercules_file}

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
      ${apollo_command} dfs -get ${one_meta}
      if [ ! -f "${meta_file_name}" ]; then
        continue;
      fi
      python /mnt/jobs/tracking/spark-nrt/bin/readMetaFile.py ${meta_file_name} ${output_file}
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
        ${apollo_command} dfs -rm ${one_meta}
        echo "${one_meta} can delete"
      else
        echo "${one_meta} can't delete"
      fi
      rm ${meta_file_name}
  done
}

./putAmsHourlyDoneToRenoAndHercules_v3.sh ${TYPE}

cd ${LOCAL_TMP_DIR}

all_meta_files="${LOCAL_TMP_DIR}/all_meta_files.txt"
epn_output_path="viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir/meta/EPN/output"
if [ "${TYPE}" == "click" ]
then
  ${apollo_command} dfs -ls ${epn_output_path}/epnnrt_scp_click  | grep .epnnrt_reno  |  grep -v "^$" | awk '{print $NF}' | grep viewfs: > ${all_meta_files}
  process_meta_file ${all_meta_files} ${apollo_file}
  ${apollo_command} dfs -ls ${epn_output_path}/epnnrt_scp_click  | grep .epnnrt_hercules  |  grep -v "^$" | awk '{print $NF}' | grep viewfs: > ${all_meta_files}
  process_meta_file ${all_meta_files} ${hercules_file}
elif [ "${TYPE}" == "imp" ]
then
    ${apollo_command} dfs -ls ${epn_output_path}/epnnrt_scp_imp  | grep .epnnrt_reno  |  grep -v "^$" | awk '{print $NF}' | grep viewfs: > ${all_meta_files}
    process_meta_file ${all_meta_files} ${apollo_file}
    ${apollo_command} dfs -ls ${epn_output_path}/epnnrt_scp_imp  | grep .epnnrt_hercules  |  grep -v "^$" | awk '{print $NF}' | grep viewfs:> ${all_meta_files}
    process_meta_file ${all_meta_files} ${hercules_file}
else
    echo "Wrong type to metaFile!"
    exit 1
fi

echo "distcpAmsToRenoAndHercules copy finish"

