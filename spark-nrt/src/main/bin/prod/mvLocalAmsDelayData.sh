#!/bin/bash

# mv imk delay data and ams delay metafile into DelayData folder
set -x

usage="Usage: mvLocalAmsDelayData.sh [type]"


apollo_command=/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs

LOCAL_TMP_PATH='/datashare/mkttracking/data/delayData';
AMS_DELAY_PATH='viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir/meta/EPN/output/delayMeta';
FLAG=false;

EPNNRT_SCP_CLICK_TESS='viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir/meta/EPN/output/epnnrt_scp_click';
EPNNRT_SCP_IMP_TESS='viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir/meta/EPN/output/epnnrt_scp_imp';
date;

today=$(date +%Y-%m-%d);
yesterday=$(date --date="${today} -1days" +%Y-%m-%d);

echo "today:$today";
echo "yesterday:$yesterday";
export HADOOP_USER_NAME=chocolate

delay_file="${LOCAL_TMP_PATH}/delay_file.txt";
ams_tmp_file="${LOCAL_TMP_PATH}/ams_tmp_file.txt";

echo "" > $delay_file;

function dealWithAmsDelayMeta() {
    type=$1;
    echo "type:$type";
    echo "" > $ams_tmp_file;
    if [ $type == 'click_tess' ]; then
      ${apollo_command} dfs -ls $EPNNRT_SCP_CLICK_TESS | grep .epnnrt_reno  |  grep -v "^$" | awk '{print $NF}' | grep viewfs: >> $ams_tmp_file
      ${apollo_command} dfs -ls $EPNNRT_SCP_CLICK_TESS | grep .epnnrt_hercules  |  grep -v "^$" | awk '{print $NF}' | grep viewfs: >> $ams_tmp_file
      DIST_DELAY_PATH="${AMS_DELAY_PATH}/click"
    elif [ $type == 'imp_tess' ]; then
      ${apollo_command} dfs -ls $EPNNRT_SCP_IMP_TESS | grep .epnnrt_reno  |  grep -v "^$" | awk '{print $NF}' | grep viewfs: >> $ams_tmp_file
      ${apollo_command} dfs -ls $EPNNRT_SCP_IMP_TESS | grep .epnnrt_hercules  |  grep -v "^$" | awk '{print $NF}' | grep viewfs: >> $ams_tmp_file
      DIST_DELAY_PATH="${AMS_DELAY_PATH}/imp"
    else
      exit 1;
    fi

    all_ams_files=`cat $ams_tmp_file | tr "\n" " "`
    for one_meta in ${all_ams_files}
    do
      echo "deal with $one_meta";
      file_name=$(basename "$one_meta");
      output_file="${LOCAL_TMP_PATH}/output_file.txt"
      meta_file_name=$(basename "${one_meta}")
      rm -f ${meta_file_name}
      ${apollo_command} dfs -get ${one_meta}
      if [ ! -f "${meta_file_name}" ]; then
        continue;
      fi
      python /datashare/mkttracking/jobs/tracking/epn-nrt/bin/readMetaFile.py ${meta_file_name} ${output_file}
      rcode=$?
      if [ ${rcode} -ne 0 ]
      then
          echo -e "Failed to parse meta file: ${meta_file_name}!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in parsing meta file!!!" -v DL-eBay-Chocolate-GC@ebay.com
          exit ${rcode}
      fi
      data_files=`cat ${output_file} | grep -v "^$" | awk '{print $NF}' | grep dw_ams`
      for one_file in ${data_files}
      do
        if [ $type == 'click_tess' ]; then
          orgDate=${one_file:81:10}
        elif [ $type == 'imp_tess' ]; then
          orgDate=${one_file:87:10}
        else
          exit 1;
        fi
        if [ $orgDate == $today -o $orgDate == $yesterday ]
        then
          echo "$file_name need not mv"
          continue;
        else
          FLAG="true"
          echo $one_meta >> $delay_file;
          echo "${apollo_command} dfs -mv $one_meta $DIST_DELAY_PATH";
          ${apollo_command} dfs -mv $one_meta $DIST_DELAY_PATH
          break;
        fi
      done
      rm ${meta_file_name};
    done
}

cd "$LOCAL_TMP_PATH/metafile"
pwd
dealWithAmsDelayMeta click_tess;
dealWithAmsDelayMeta imp_tess;

cat $delay_file;

echo "FLAG:$FLAG"
if [ $FLAG == "true" ]; then
    echo "Have delay data";
    all_delay_files=`cat ${delay_file}`
    echo -e "Data delayed more than two days: ${all_delay_files}" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[Imk or Ams Data Delay] Data delayed more than two days!!!" -v DL-eBay-Chocolate-GC@ebay.com
fi