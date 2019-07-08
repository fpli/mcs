#!/bin/bash

# Put Reno kw lkp files to SLC.
# Input:    Reno Hadoop
#           /apps/hdmi-technology/b_marketing_psdm/production/PSDM_META_TABLES/DW_KWDM_KW_LKP/1/1
# Output:   SLC
#           /apps/kw_lkp
# Schedule: /3 * ? * *

set -x

usage="Usage: putRenoKwLkpToSlc.sh [renoSrcDir] [slcDestDir] [localTmpDir]"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

export HADOOP_USER_NAME=chocolate

HOST_NAME=`hostname -f`
kinit -kt /datashare/mkttracking/tools/keytab-tool/keytab/b_marketing_tracking.${HOST_NAME}.keytab  b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM

RENO_SRC_DIR=$1
SLC_DEST_DIR=$2
LOCAL_TMP_DIR=$3

cd ${LOCAL_TMP_DIR}

today=$(date +%Y-%m-%d)

destFolder=${SLC_DEST_DIR}/${today}
# create dest folder if not exists
hdfs dfs -mkdir -p ${destFolder}

srcFolder="viewfs://apollo-rno${RENO_SRC_DIR}/${today}_00-00-00"

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -d ${srcFolder}
rcode=$?
if [[ ${rcode} ne 0 ]]; then
    echo "${srcFolder} not exist"
    exit ${rcode}
fi

# get all done files in last two days
tmp_index_file=reno_kw_to_slc_index.txt
rm -f ${tmp_index_file}

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls -R ${srcFolder} | grep -v "^$" | awk '{print $NF}' >> ${tmp_index_file}

files_size=`cat ${tmp_index_file} | wc -l`
echo "start copy files size:"${files_size}

all_files=`cat ${tmp_index_file} | tr "\n" " "`
for one_file in ${all_files}
do
#   eg.
    file_name=$(basename "$one_file")
    rm -f ${file_name}
#   get one data file from reno
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -get ${one_file}
    rcode=$?
    if [ ${rcode} -ne 0 ]
    then
        echo "Fail to get from HDFS, please check!!!"
        exit ${rcode}
    fi
#   check if file exists
    hdfs dfs -test -e ${destFolder}/${file_name}
    fileExist=$?
    if [[ ${fileExist} -ne 0 ]]; then
#       max 3 times put file to slc
        retry=1
        rcode=1
        until [[ ${retry} -gt 3 ]]
        do
            command="hdfs dfs -put ${file_name} ${destFolder}/"
            ${command}
            rcode=$?
            if [ ${rcode} -eq 0 ]
            then
                break
            else
                echo "Faild to upload to SLC, retrying ${retry}"
                retry=`expr ${retry} + 1`
             fi
        done
        if [ ${rcode} -ne 0 ]
        then
            echo "Fail to upload to SLC, please check!!!"
            exit ${rcode}
        fi
    else
        echo "${file_name} existed"
    fi
#    remove local done file
    rm -f ${file_name}
    echo "finish copy file:"${file_name}
done

rm -f ${tmp_index_file}
echo "finish copy files size:"${files_size}

KW_LKP_LATEST_PATH=hdfs://slickha/apps/kw_lkp/latest_path

tmp_path_file=latest_path
rm -f ${tmp_path_file}

echo "${destFolder}" > ${tmp_path_file}

hdfs dfs -test -e ${KW_LKP_LATEST_PATH}
fileExist=$?
if [[ ${fileExist} -eq 0 ]]; then
#       max 3 times update path file
    retry=1
    rcode=1
    until [[ ${retry} -gt 3 ]]
    do
        command="hdfs dfs -put -f ${tmp_path_file} ${KW_LKP_LATEST_PATH}"
        ${command}
        rcode=$?
        if [ ${rcode} -eq 0 ]
        then
            break
        else
            echo "Faild to upload to SLC, retrying ${retry}"
            retry=`expr ${retry} + 1`
         fi
    done
    if [ ${rcode} -ne 0 ]
    then
        echo "Fail to upload to SLC, please check!!!"
        exit ${rcode}
    fi
else
    echo "${KW_LKP_LATEST_PATH} not existed"
fi

rm -f ${tmp_path_file}
echo "finish update path file:"${KW_LKP_LATEST_PATH}