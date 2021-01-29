#!/bin/bash
# Convert choco_data.ams_click table to choco_data.ams_click_v2 table,
# run on SLC server slcchocolatepits-1242736
# Input: beginDate ---- 20201009
#        endDate ----- 20201010
set -x
usage="Usage: historicalAMSClickDataReformat.sh [beginDate] [endDate]"

if [ $# -lt 2 ]; then
  echo "$usage"
  exit 1
fi

BEGIN_DATE=${1}
END_DATE=${2}

echo "BEGIN_DATE:$BEGIN_DATE";
echo "END_DATE:$END_DATE";

if [[ -z $BEGIN_DATE ||  -z $END_DATE || $END_DATE < $BEGIN_DATE ]]
then
    echo "$usage";
    exit 1;
fi

current_date=$BEGIN_DATE;
echo "current_date:$current_date";

LOCAL_PATH='/datashare/mkttracking/jobs/amsReformat/click'
AMS_TMP_PATH='/apps/b_marketing_tracking/delta_test/ams_click_tmp'

AMS_NEW_PATH_APOLLO='/apps/b_marketing_tracking/chocolate/epnnrt/click_v2'
COMMAND_HIVE_APOLLO="/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive "
COMMAND_HDFS_APOLLO="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs "

AMS_NEW_PATH_HERCULES='/sys/edw/imk/im_tracking/epn/ams_click_v2/snapshot'
COMMAND_HIVE_HERCULES="/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive "
COMMAND_HDFS_HERCULES="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs "

ENV_PATH='/datashare/mkttracking/tools/cake'
JOB_NAME='DistcpAMSClickNewToRenoAndHerculesBatchJob'
echo "ENV_PATH:${ENV_PATH}"
echo "JOB_NAME:${JOB_NAME}"

# shellcheck disable=SC2164
cd ${LOCAL_PATH}
pwd
while [[ $current_date -le $END_DATE ]]; do
    echo "current_date:$current_date";
    echo "begin deal with data in $current_date";
    click_dt=${current_date:0:4}-${current_date:4:2}-${current_date:6}
    echo "click_dt:$click_dt";
    done_file="./done/done_tmp.txt"
    echo "click_dt:$click_dt" > $done_file;
    echo "start time:$(date '+%Y-%m-%d %H:%M:%S')" >> $done_file;

    # rm ams_click files,avoid duplicate data
    $COMMAND_HDFS_APOLLO -test -f "${AMS_TMP_PATH}/*.parquet"
    # shellcheck disable=SC2181
    if [ $? -eq 0 ]; then
        $COMMAND_HDFS_APOLLO -rm "${AMS_TMP_PATH}/*.parquet"
        hdfs_result_code=$?
        echo "hdfs exit code: $hdfs_result_code"
        if [ $hdfs_result_code -ne 0 ]; then
            echo "hdfs -rm ams_click_tmp fail:${click_dt}";
            exit $hdfs_result_code;
        fi
    fi

    # reformat ams_click data to ams_click_v2
    echo "begin reformat data:${click_dt}";
    reformat_sql_file="./tmp/ams_click_to_tmp_by_click_dt.sql";
    sed "s/#{click_dt}/${click_dt}/g" ams_click_to_tmp_by_click_dt_template.sql > $reformat_sql_file;
    cat $reformat_sql_file;
    ../amsReformat.sh $reformat_sql_file;
    spark_result_code=$?
    echo "spark exit code: $spark_result_code"
    if [ $spark_result_code -ne 0 ]; then
        echo "reformat data fail:${click_dt}";
        exit $spark_result_code;
    fi
    echo "end reformat data:${click_dt}";

    # rm choco_data.ams_click_v2 files under the current date partition
    # mv choco_data.ams_click_tmp files to choco_data.ams_click_v2's current partition
    ## Create date dir if it doesn't exist
    $COMMAND_HDFS_APOLLO -test -f "${AMS_NEW_PATH_APOLLO}/click_dt=${click_dt}/*.parquet"
    # shellcheck disable=SC2181
    if [ $? -eq 0 ]; then
        $COMMAND_HDFS_APOLLO -rm "${AMS_NEW_PATH_APOLLO}/click_dt=${click_dt}/*.parquet";
    else
        echo "Create folder for ${click_dt}"
        $COMMAND_HDFS_APOLLO -mkdir "${AMS_NEW_PATH_APOLLO}/click_dt=${click_dt}"
    fi


    $COMMAND_HDFS_HERCULES -test -f "${AMS_NEW_PATH_HERCULES}/click_dt=${click_dt}/*.parquet"
    if [ $? -eq 0 ]; then
        $COMMAND_HDFS_HERCULES -rm "${AMS_NEW_PATH_HERCULES}/click_dt=${click_dt}/*.parquet";
    else
        echo "Create folder for ${click_dt}"
        $COMMAND_HDFS_HERCULES -mkdir "${AMS_NEW_PATH_HERCULES}/click_dt=${click_dt}"
    fi

    $COMMAND_HIVE_APOLLO -e "set hive.msck.path.validation=ignore; ALTER TABLE choco_data.ams_click_v2 ADD IF NOT EXISTS PARTITION (click_dt='${click_dt}')"
    $COMMAND_HIVE_HERCULES -e "set hive.msck.path.validation=ignore; ALTER TABLE im_tracking.ams_click_v2 ADD IF NOT EXISTS PARTITION (click_dt='${click_dt}')"
    # mv ams_click_tmp files to ams_click_v2's current partition
    $COMMAND_HDFS_APOLLO -mv ${AMS_TMP_PATH}/*.parquet "${AMS_NEW_PATH_APOLLO}/click_dt=${click_dt}/"

    RNO_PATH="hdfs://apollo-rno${AMS_NEW_PATH_APOLLO}/click_dt=${click_dt}"
    HERCULES_PATH="hdfs://hercules${AMS_NEW_PATH_HERCULES}"
    /datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}
    echo "finish time:$(date '+%Y-%m-%d %H:%M:%S')" >> $done_file;
    cat $done_file > "./done/${click_dt}.txt";
    echo "end deal with data in $current_date";
    current_date=$(date -d"${current_date} 1 days" +"%Y%m%d");
done