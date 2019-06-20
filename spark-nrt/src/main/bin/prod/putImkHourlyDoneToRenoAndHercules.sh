#!/bin/bash

# Put imk hourly done from chocolate hadoop to Apollo RNO and Hercules.
# Input:    SLC Hadoop
#           /apps/tracking-events/watch
# Output:   Apollo RNO
#           /apps/b_marketing_tracking/watch
#           Hercules
#           /apps/b_marketing_tracking/watch
# Schedule: /10 * ? * *

usage="Usage: putImkHourlyDoneToRenoAndHercules.sh [srcDir] [renoDestDir] [herculesDestDir] [localTmpDir]"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

export HADOOP_USER_NAME=chocolate

HOST_NAME=`hostname -f`
kinit -kt /datashare/mkttracking/tools/keytab-tool/keytab/b_marketing_tracking.${HOST_NAME}.keytab  b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM

SRC_DIR=$1
RENO_DEST_DIR=$2
HERCULES_DEST_DIR=$3
LOCAL_TMP_DIR=$4

cd ${LOCAL_TMP_DIR}

today=$(date +%Y%m%d)
yesterday=$(date -d "yesterday" +%Y%m%d)

#get all done files in last two days
tmp_done_file=imk_to_reno_and_hercules_done.txt
hdfs dfs -ls -R ${SRC_DIR}/${yesterday} > ${tmp_done_file}
hdfs dfs -ls -R ${SRC_DIR}/${today} >> ${tmp_done_file}

files_size=`cat ${tmp_done_file} | wc -l`
echo "start copy done files size:"${files_size}

all_files=`cat ${tmp_done_file} | tr "\n" " "`
for one_file in ${all_files}
do
#   eg. imk_rvr_trckng_event_hourly.done.201904251100000000
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

    date=${file_name:33:8}
    destFolder=${RENO_DEST_DIR}/${date}
    herculesFolder=${HERCULES_DEST_DIR}/${date}
#    create dest folder if not exists, folder in hercules should be created in advance
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p ${destFolder}
#   copy new done file
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -e ${destFolder}/${file_name}
    fileExist=$?
    if [[ ${fileExist} -ne 0 ]]; then
#       max 3 times put done file to reno and hercules
        retry=1
        rcode=1
        until [[ ${retry} -gt 3 ]]
        do
            command_1="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -put -f ${file_name} ${destFolder}/"
            command_2="/datashare/mkttracking/tools/cake/bin/distcp.sh viewfs://apollo-rno${destFolder}/${file_name} hdfs://hercules${herculesFolder}/ putImkDoneToHercules"
            ${command_1} && ${command_2}
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
        echo "${file_name} done file existed"
    fi
#    remove local done file
    rm -f ${file_name}
    echo "finish copy file:"${file_name}
done
rm -f ${tmp_done_file}
echo "finish copy files size:"${files_size}
