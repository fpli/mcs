#!/bin/bash
# Convert IMK_RVR_TRCKNG_EVENT table and B table to IMK_RVR_TRCKNG_EVENT_DTL table,
# and migrate the data in IMK_RVR_TRCKNG_EVENT,IMK_RVR_TRCKNG_EVENT_DTL table to imk_rvr_trckng_event_v2 table.
# run on SLC server slcchocolatepits-1242736
# Input: beginDate ---- 20201009
#        endDate ----- 20201010
set -x
usage="Usage: historicalIMKDataReformat.sh [beginDate] [endDate]"

if [ $# -lt 2 ]; then
  echo $usage
  exit 1
fi

BEGIN_DATE=${1}
END_DATE=${2}

echo "BEGIN_DATE:$BEGIN_DATE";
echo "END_DATE:$END_DATE";

if [[ -z $BEGIN_DATE ||  -z $END_DATE || $END_DATE < $BEGIN_DATE ]]
then
    echo $usage;
    exit 1;
fi

current_date=$BEGIN_DATE;
echo "current_date:$current_date";

LOCAL_PATH='/datashare/mkttracking/jobs/imkReformate'
IMK_TMP_PATH='/apps/b_marketing_tracking/delta_test/imk_tmp'

IMK_NEW_PATH_APOLLO='/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_v2'
COMMAND_HIVE_APOLLO="/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive "
COMMAND_HDFS_APOLLO="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs "

IMK_NEW_PATH_HERCULES='/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_v2/snapshot'
COMMAND_HIVE_HERCULES="/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive "
COMMAND_HDFS_HERCULES="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs "

ENV_PATH='/datashare/mkttracking/tools/cake'
JOB_NAME='DistcpImkNewToRenoAndHerculesBatchJob'
echo "ENV_PATH:${ENV_PATH}"
echo "JOB_NAME:${JOB_NAME}"

cd ${LOCAL_PATH}
pwd
while [[ $current_date -le $END_DATE ]]; do
    echo "current_date:$current_date";
    echo "begin deal with data in $current_date";
    dt=${current_date:0:4}-${current_date:4:2}-${current_date:6}
    echo "dt:$dt";
    done_file="./done/done_tmp.txt"
    echo "dt:$dt" > $done_file;
    echo "start time:$(date '+%Y-%m-%d %H:%M:%S')" >> $done_file;

    # rm IMK_RVR_TRCKNG_EVENT_TMP files,avoid duplicate data
    $COMMAND_HDFS_APOLLO -test -f "${IMK_TMP_PATH}/*.parquet"
    if [ $? -eq 0 ]; then
        $COMMAND_HDFS_APOLLO -rm "${IMK_TMP_PATH}/*.parquet"
        hdfs_result_code=$?
        echo "hdfs exit code: $hdfs_result_code"
        if [ $hdfs_result_code -ne 0 ]; then
            echo "hdfs -rm imk_tmp fail:${dt}";
            exit $hdfs_result_code;
        fi
    fi

    # reformate IMK_RVR_TRCKNG_EVENT and imk_rvr_trckng_event_v2 data to IMK_RVR_TRCKNG_EVENT_TMP
    echo "begin reformate data:${dt}";
    reformat_sql_file="./tmp/imk_rvr_trckng_event_and_dtl_to_tmp_by_dt.sql";
    sed "s/#{dt}/${dt}/g" imk_rvr_trckng_event_and_dtl_to_tmp_by_dt_template.sql > $reformat_sql_file;
    cat $reformat_sql_file;
    ./imkReformat.sh $reformat_sql_file;
    spark_result_code=$?
    echo "spark exit code: $spark_result_code"
    if [ $spark_result_code -ne 0 ]; then
        echo "reformate data fail:${dt}";
        exit $spark_result_code;
    fi
    echo "end reformate data:${dt}";

    # rm imk_rvr_trckng_event_v2 files under the current date partition
    # mv IMK_RVR_TRCKNG_EVENT_TMP files to imk_rvr_trckng_event_v2's current partition
    ## Create date dir if it doesn't exist
    $COMMAND_HDFS_APOLLO -test -f "${IMK_NEW_PATH_APOLLO}/dt=${dt}/*.parquet"
    if [ $? -eq 0 ]; then
        $COMMAND_HDFS_APOLLO -rm "${IMK_NEW_PATH_APOLLO}/dt=${dt}/*.parquet";
    else
        echo "Create folder for ${dt}"
        $COMMAND_HDFS_APOLLO -mkdir "${IMK_NEW_PATH_APOLLO}/dt=${dt}"
    fi
    $COMMAND_HDFS_HERCULES -test -f "${IMK_NEW_PATH_HERCULES}/dt=${dt}/*.parquet"
    if [ $? -eq 0 ]; then
        $COMMAND_HDFS_HERCULES -rm "${IMK_NEW_PATH_HERCULES}/dt=${dt}/*.parquet";
    else
        echo "Create folder for ${dt}"
        $COMMAND_HDFS_HERCULES -mkdir "${IMK_NEW_PATH_HERCULES}/dt=${dt}"
    fi
    $COMMAND_HIVE_APOLLO -e "set hive.msck.path.validation=ignore; ALTER TABLE choco_data.imk_rvr_trckng_event_v2 ADD IF NOT EXISTS PARTITION (dt='${dt}')"
    $COMMAND_HIVE_HERCULES -e "set hive.msck.path.validation=ignore; ALTER TABLE im_tracking.imk_rvr_trckng_event_v2 ADD IF NOT EXISTS PARTITION (dt='${dt}')"
    # mv IMK_RVR_TRCKNG_EVENT_TMP files to imk_rvr_trckng_event_v2's current partition
    $COMMAND_HDFS_APOLLO -mv ${IMK_TMP_PATH}/*.parquet "${IMK_NEW_PATH_APOLLO}/dt=${dt}/"

    RNO_PATH="hdfs://apollo-rno${IMK_NEW_PATH_APOLLO}/dt=${dt}"
    HERCULES_PATH="hdfs://hercules${IMK_NEW_PATH_HERCULES}"
    /datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}

    echo "finish time:$(date '+%Y-%m-%d %H:%M:%S')" >> $done_file;
    cat $done_file > "./done/${dt}.txt";
    echo "end deal with data in $current_date";
    current_date=$(date -d"${current_date} 1 days" +"%Y%m%d");
done