#!/bin/bash

# mv imk delay data and ams delay metafile into DelayData folder
set -x

usage="Usage: mvLocalImkAndAmsDelayData.sh [type]"

LOCAL_PATH='/datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod';
LOCAL_TMP_PATH='/datashare/mkttracking/data/delayData';
IMK_DELAY_PATH='hdfs://slickha/apps/tracking-events/imkDelayData';
AMS_DELAY_PATH='hdfs://elvisha/apps/tracking-events-workdir/meta/EPN/output/delayMeta';
FLAG=false;

echo "IMK_DELAY_PATH:$IMK_DELAY_PATH";
echo "IMK_DTL_DELAY_PATH:$IMK_DTL_DELAY_PATH";
echo "AMS_CLICK_DELAY_PATH:$AMS_CLICK_DELAY_PATH";
echo "AMS_IMP_DELAY_PATH:$AMS_IMP_DELAY_PATH";

IMK_CRABTRANSFORM_PATH='hdfs://slickha/apps/tracking-events/crabTransform/imkOutput';
IMK_IMKTRANSFORM_PATH='hdfs://slickha/apps/tracking-events/imkTransform/imkOutput';
DTL_CRABTRANSFORM_PATH='hdfs://slickha/apps/tracking-events/crabTransform/dtlOutput';
DTL_IMKTRANSFORM_PATH='hdfs://slickha/apps/tracking-events/imkTransform/dtlOutput';

IMK_V2_IMKTRANSFORM_PATH='hdfs://slickha/apps/tracking-events-imk/imkTransform/imkOutput';

EPNNRT_SCP_CLICK='hdfs://elvisha/apps/tracking-events-workdir/meta/EPN/output/epnnrt_scp_click';
EPNNRT_SCP_IMP='hdfs://elvisha/apps/tracking-events-workdir/meta/EPN/output/epnnrt_scp_imp';
EPNNRT_SCP_CLICK_V2='hdfs://slickha/apps/tracking-events-workdir-v2/meta/EPN/output/epnnrt_scp_click';
EPNNRT_SCP_IMP_V2='hdfs://slickha/apps/tracking-events-workdir-v2/meta/EPN/output/epnnrt_scp_imp';
date;

today=$(date +%Y-%m-%d);
yesterday=$(date --date="${today} -1days" +%Y-%m-%d);

echo "today:$today";
echo "yesterday:$yesterday";
export HADOOP_USER_NAME=chocolate

cd $LOCAL_PATH;
pwd

delay_file="${LOCAL_TMP_PATH}/delay_file.txt";
imk_tmp_file="${LOCAL_TMP_PATH}/imk_tmp_file.txt";
ams_tmp_file="${LOCAL_TMP_PATH}/ams_tmp_file.txt";

echo "" > $delay_file;

function dealWithImkDelayData() {
  type=$1;
  echo "type:$type";
  echo "" > $imk_tmp_file;
  if [ $type == 'imk' ]; then
      hdfs dfs -ls ${IMK_CRABTRANSFORM_PATH} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" > ${imk_tmp_file}
      hdfs dfs -ls ${IMK_IMKTRANSFORM_PATH} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" >> ${imk_tmp_file}
      DIST_DELAY_PATH="${IMK_DELAY_PATH}/imk"
  elif [ $type == 'dtl' ]; then
      hdfs dfs -ls ${DTL_CRABTRANSFORM_PATH} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" > ${imk_tmp_file}
      hdfs dfs -ls ${DTL_IMKTRANSFORM_PATH} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" >> ${imk_tmp_file}
      DIST_DELAY_PATH="${IMK_DELAY_PATH}/dtl"
  elif [ $type == 'imk_v2' ]; then
      hdfs dfs -ls ${IMK_V2_IMKTRANSFORM_PATH} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" >> ${imk_tmp_file}
      DIST_DELAY_PATH="${IMK_DELAY_PATH}/imk_v2"
  else
    exit 1;
  fi
  all_imk_files=`cat ${imk_tmp_file} | tr "\n" " "`
  for one_file in ${all_imk_files}
  do
    echo "deal with $one_file";
    file_name=$(basename "$one_file");
    echo "file_name:${file_name}";
    orgDate=${file_name:15:10}
    if [ $orgDate == $today -o $orgDate == $yesterday ]
    then
      echo "$file_name need not mv"
      continue;
    else
      FLAG="true"
      echo $one_file >> $delay_file;
      echo "hdfs dfs -mv $one_file $DIST_DELAY_PATH";
      hdfs dfs -mv $one_file $DIST_DELAY_PATH
    fi
  done
}

function dealWithAmsDelayMeta() {
    type=$1;
    echo "type:$type";
    echo "" > $ams_tmp_file;
    if [ $type == 'click' ]; then
      hdfs dfs -ls $EPNNRT_SCP_CLICK | grep .epnnrt_reno  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: > $ams_tmp_file
      hdfs dfs -ls $EPNNRT_SCP_CLICK | grep .epnnrt_hercules  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: >> $ams_tmp_file
      DIST_DELAY_PATH="${AMS_DELAY_PATH}/click"
    elif [ $type == 'click_v2' ]; then
      hdfs dfs -ls $EPNNRT_SCP_CLICK_V2 | grep .epnnrt_reno  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: >> $ams_tmp_file
      hdfs dfs -ls $EPNNRT_SCP_CLICK_V2 | grep .epnnrt_hercules  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: >> $ams_tmp_file
      DIST_DELAY_PATH="${AMS_DELAY_PATH}/click"
    elif [ $type == 'imp' ]; then
      hdfs dfs -ls $EPNNRT_SCP_IMP | grep .epnnrt_reno  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: > $ams_tmp_file
      hdfs dfs -ls $EPNNRT_SCP_IMP | grep .epnnrt_hercules  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: >> $ams_tmp_file
      DIST_DELAY_PATH="${AMS_DELAY_PATH}/imp"
     elif [ $type == 'imp_v2' ]; then
      hdfs dfs -ls $EPNNRT_SCP_IMP_V2 | grep .epnnrt_reno  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: >> $ams_tmp_file
      hdfs dfs -ls $EPNNRT_SCP_IMP_V2 | grep .epnnrt_hercules  |  grep -v "^$" | awk '{print $NF}' | grep hdfs: >> $ams_tmp_file
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
      hdfs dfs -get ${one_meta}
      if [ ! -f "${meta_file_name}" ]; then
        continue;
      fi
      python /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/readMetaFile.py ${meta_file_name} ${output_file}
      rcode=$?
      if [ ${rcode} -ne 0 ]
      then
          echo -e "Failed to parse meta file: ${meta_file_name}!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in parsing meta file!!!" -v DL-eBay-Chocolate-GC@ebay.com
          exit ${rcode}
      fi
      data_files=`cat ${output_file} | grep -v "^$" | awk '{print $NF}' | grep dw_ams`
      for one_file in ${data_files}
      do
        if [ $type == 'click' ]; then
          orgDate=${one_file:39:10}
        elif [ $type == 'click_v2' ]; then
          orgDate=${one_file:42:10}
        elif [ $type == 'imp' ]; then
          orgDate=${one_file:44:10}
        elif [ $type == 'imp_v2' ]; then
          orgDate=${one_file:47:10}
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
          echo "hdfs dfs -mv $one_meta $DIST_DELAY_PATH";
          hdfs dfs -mv $one_meta $DIST_DELAY_PATH
          break;
        fi
      done
      rm ${meta_file_name};
    done
}

cd "$LOCAL_TMP_PATH/metafile"
pwd
dealWithAmsDelayMeta click;
dealWithAmsDelayMeta click_v2;
dealWithAmsDelayMeta imp;
dealWithAmsDelayMeta imp_v2;

cat $delay_file;

echo "FLAG:$FLAG"
if [ $FLAG == "true" ]; then
    echo "Have delay data";
    all_delay_files=`cat ${delay_file}`
    echo -e "Data delayed more than two days: ${all_delay_files}" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[Imk or Ams Data Delay] Data delayed more than two days!!!" -v DL-eBay-Chocolate-GC@ebay.com
fi