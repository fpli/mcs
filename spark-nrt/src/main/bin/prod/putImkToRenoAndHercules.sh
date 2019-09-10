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
#           /apps/b_marketing_tracking/IMK_RVR_TRCKNG_EVENT/imk_rvr_trckng_event
#           /apps/b_marketing_tracking/IMK_RVR_TRCKNG_EVENT/imk_rvr_trckng_event_dtl
# Schedule: /3 * ? * *

set -x

usage="Usage: putImkToRenoAndHercules.sh [srcDir] [localTmpDir] [renoMiddleDir] [renoDestDir] [herculesMiddleDir] [herculesDestDir]"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

export HADOOP_USER_NAME=chocolate

SRC_DIR=$1
LOCAL_TMP_DIR=$2
RENO_MID_DIR=$3
RENO_DEST_DIR=$4
HERCULES_MID_DIR=$5
HERCULES_DEST_DIR=$6

cd ${LOCAL_TMP_DIR}

#get file list from chocolate hdfs
tmp_file=imk_to_reno.txt
hdfs dfs -ls -R ${SRC_DIR} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" > ${tmp_file}

files_size=`cat ${tmp_file} | wc -l`
echo "start copy files size:"${files_size}

all_files=`cat ${tmp_file} | tr "\n" " "`
for one_file in ${all_files}
do
    file_name=$(basename "$one_file")
    rm -f ${file_name}
#   get one data file from chocolate hdfs
    hdfs dfs -get ${one_file}
    rcode=$?
    if [ ${rcode} -ne 0 ]
    then
        echo "Fail to get from HDFS, please check!!!"
        exit ${rcode}
    fi

    orgDate=${file_name:15:10}
    date=${orgDate}
    renoDestFolder=${RENO_DEST_DIR}/dt=${date}
    herculesDestFolder=${HERCULES_DEST_DIR}/dt=${date}
#    create dest folder if not exists
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p ${renoDestFolder}
    /datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -mkdir -p ${herculesDestFolder}
    if [[ -s ${file_name} ]];
    then
#        max 3 times put data to reno middle and mv data to reno dest folder
        retry=1
        rcode=1
        until [[ ${retry} -gt 3 ]]
        do
#           to Rno
            command_1="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -put -f ${file_name} ${RENO_MID_DIR}/"
            command_2="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -rm -f ${renoDestFolder}/${file_name}"
            command_3="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mv ${RENO_MID_DIR}/${file_name} ${renoDestFolder}/"
#           to Hercules
            command_4="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -put -f ${file_name} ${HERCULES_MID_DIR}/"
            command_5="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -rm -f ${herculesDestFolder}/${file_name}"
            command_6="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -mv ${HERCULES_MID_DIR}/${file_name} ${herculesDestFolder}/"
            ${command_1} && ${command_2} && ${command_3} && ${command_4} && ${command_5} && ${command_6}
            rcode=$?
            if [ ${rcode} -eq 0 ]
            then
                break
            else
                echo "Faild to upload to Reno and Hercules, retrying ${retry}"
                retry=`expr ${retry} + 1`
             fi
        done
        if [ ${rcode} -ne 0 ]
        then
            echo "Fail to upload to Reno and Hercules, please check!!!"
            exit ${rcode}
        fi
    else
#        not put empty data file to RENO and Hercules
        echo "empty data file"
    fi
#    remove local and chocolate hdfs data file
    rm -f ${file_name}
    hdfs dfs -rm ${one_file}
    rcode=$?
    if [ ${rcode} -ne 0 ]
    then
        echo "Fail to remove from HDFS, please check!!!"
        exit ${rcode}
    fi
    echo "finish copy file:"${file_name}
done
rm -f ${tmp_file}
echo "finish copy files size:"${files_size}

echo "======================== Add partition to Hive ========================"
partition_date=$(date +%Y-%m-%d)
retry_add=1
rcode_add=1
until [[ ${retry_add} -gt 3 ]]
do
    /datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -e "set hive.msck.path.validation=ignore; ALTER TABLE choco_data.imk_rvr_trckng_event ADD IF NOT EXISTS PARTITION (dt='${partition_date}')" &&
    /datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -e "set hive.msck.path.validation=ignore; ALTER TABLE choco_data.imk_rvr_trckng_event_dtl ADD IF NOT EXISTS PARTITION (dt='${partition_date}')" &&
    /datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -e "set hive.msck.path.validation=ignore; ALTER TABLE im_tracking.imk_rvr_trckng_event ADD IF NOT EXISTS PARTITION (dt='${partition_date}')" &&
    /datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -e "set hive.msck.path.validation=ignore; ALTER TABLE im_tracking.imk_rvr_trckng_event_dtl ADD IF NOT EXISTS PARTITION (dt='${partition_date}')"
    rcode_add=$?
    if [ ${rcode_add} -eq 0 ]
    then
        break
    else
        echo "Failed to add ${partition_date} partition to hive."
        retry_add=$(expr ${retry_add} + 1)
    fi
done
if [ ${rcode_add} -ne 0 ]
then
    echo -e "Failed to add ${partition_date} partition on hive, please check!!!"
    exit ${rcode_add}
fi