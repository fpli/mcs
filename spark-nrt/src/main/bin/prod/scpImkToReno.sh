#!/bin/bash

usage="Usage: scpImkToReno.sh [srcDir] [renoDir] [tmpDir]"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

export HADOOP_USER_NAME=chocolate

HOST_NAME=`hostname -f`
kinit -kt /datashare/mkttracking/tools/keytab-tool/keytab/b_marketing_tracking.${HOST_NAME}.keytab  b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM

SRC_DIR=$1
RENO_DIR=$2
TMP_DIR=$3

cd ${TMP_DIR}

tmp_file=imk_to_reno.txt

hdfs dfs -ls -R ${SRC_DIR} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" > ${tmp_file}

files_size=`cat ${tmp_file} | wc -l`
echo "start copy files size:"${files_size}

all_files=`cat ${tmp_file} | tr "\n" " "`
for one_file in ${all_files}
do
    file_name=$(basename "$one_file")
    rm -f ${file_name}
    hdfs dfs -get ${one_file}
    rcode=$?
    if [ $rcode -ne 0 ]
    then
        echo "Fail to get from HDFS, please check!!!"
        exit ${rcode}
    fi

    orgDate=${file_name:15:10}
    date=${orgDate//-/}
    destFolder=${RENO_DIR}/dt=${date}

    /apache/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p ${destFolder}
    if [[ -s ${file_name} ]];
    then
        /apache/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -put -f ${file_name} ${destFolder}/
        rcode=$?
        if [ $rcode -ne 0 ]
        then
            echo "Fail to upload to Reno, please check!!!"
            exit ${rcode}
        fi
    else
        echo "empty data file"
    fi

    rm -f ${file_name}
    hdfs dfs -rm ${one_file}
    rcode=$?
    if [ $rcode -ne 0 ]
    then
        echo "Fail to remove from HDFS, please check!!!"
        exit ${rcode}
    fi
    echo "finish copy file:"${file_name}
done
rm -f ${tmp_file}
echo "finish copy files size:"${files_size}
