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

usage="Usage: putImkToRenoAndHercules.sh [srcDir] [renoMiddleDir] [renoDestDir] [localTmpDir]"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

export HADOOP_USER_NAME=chocolate

HOST_NAME=`hostname -f`
kinit -kt /datashare/mkttracking/tools/keytab-tool/keytab/b_marketing_tracking.${HOST_NAME}.keytab  b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM

SRC_DIR=$1
RENO_MID_DIR=$2
RENO_DEST_DIR=$3
LOCAL_TMP_DIR=$4
HERCULES_DEST_DIR=$5

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
    date=${orgDate//-/}
    destFolder=${RENO_DEST_DIR}/dt=${date}
    herculesFolder=${HERCULES_DEST_DIR}/dt=${date}
#    create dest folder if not exists, folder in hercules should be created in advance
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p ${destFolder}
    if [[ -s ${file_name} ]];
    then
#        max 3 times put data to reno middle and mv data to reno dest folder
        retry=1
        rcode=1
        until [[ ${retry} -gt 3 ]]
        do
            command_1="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -put -f ${file_name} ${RENO_MID_DIR}/"
            command_2="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -rm -f ${destFolder}/${file_name}"
            command_3="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mv ${RENO_MID_DIR}/${file_name} ${destFolder}/"
            command_4="/datashare/mkttracking/tools/cake/bin/distcp_by_optimus.sh viewfs://apollo-rno${destFolder}/${file_name} hdfs://hercules${herculesFolder}/ putImkToHercules"
            ${command_1} && ${command_2} && ${command_3} && ${command_4}
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
