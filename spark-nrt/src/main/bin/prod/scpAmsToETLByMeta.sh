#!/bin/bash

# read output meta files by channel and action, scp data files to ETL server

WORK_DIR=$1
CHANNEL=$2
ACTION=$3
META_SUFFIX=$4
KEY_LOCATION=$5
ETL_ACCOUNT_LOCATION=$6
SCP_DONE_FILE=$7

function process_one_meta(){
    meta_file=$1
    output_file=output_file.txt
    meta_file_name=$(basename "$meta_file")
    rm -f ${meta_file_name}

#    get one meta file from chocolate hdfs and parse it
    hdfs dfs -get ${meta_file}
    python ../readMetaFile.py ${meta_file_name} ${output_file}
    rcode=$?
    if [ ${rcode} -ne 0 ]
    then
        echo "Fail to parse meta file: ${meta_file_name}"
        exit ${rcode}
    fi
    files_size=`cat ${output_file} | wc -l`
    echo "start scp files size:"${files_size}

    data_files=`cat ${output_file} | tr "\n" " "`
    while read -r date_file; do
        data_file=`echo ${date_file} | cut -d" " -f2`
        data_file_name=$(basename "$data_file")
        rm -f data_file_name
        hdfs dfs -get ${data_file}
        touch ${data_file_name}'.done'

        command_1="scp -i ${KEY_LOCATION} ${data_file_name} ${ETL_ACCOUNT_LOCATION}/${data_file_name}"
        command_2="echo 1"
        if [ $2 = "YES" ]
        then
            command_2="scp -i ${KEY_LOCATION} ${data_file_name}.done ${ETL_ACCOUNT_LOCATION}/${data_file_name}.done"
        fi
        retry=1
        rcode=1
        until [[ ${retry} -gt 3 ]]
        do
            ${command_1} && ${command_2}
            rcode=$?
            if [ ${rcode} -eq 0 ]
            then
                echo "success scp to ETL: "${data_file_name}
                break
            else
                echo "Faild to scp to ETL, retrying ${retry}"
                retry=`expr ${retry} + 1`
             fi
        done
        rm -f ${data_file_name}
        if [ ${rcode} -ne 0 ]
        then
            echo "Fail to scp to ETL, please check!!!"
            exit ${rcode}
        fi
    done < "$output_file"
    rm -f ${output_file}
    echo "finish scp files size:"${files_size}
}

export HADOOP_USER_NAME=chocolate

meta_dir=${WORK_DIR}'/meta/'${CHANNEL}'/output/'${ACTION}
tmp_dir='tmp_scp_to_etl_'${CHANNEL}'_'${ACTION}
mkdir -p ${tmp_dir}
cd ${tmp_dir}

all_meta_files=all_meta_files.txt
hdfs dfs -ls ${meta_dir} | grep -v "^$" | awk '{print $NF}' | grep ${META_SUFFIX} > ${all_meta_files}

files_size=`cat ${all_meta_files} | wc -l`
echo "start process meta files size:"${files_size}

all_files=`cat ${all_meta_files} | tr "\n" " "`
for one_meta in ${all_files}
do
    process_one_meta ${one_meta} ${SCP_DONE_FILE}
    rcode=$?
    if [ ${rcode} -ne 0 ]
    then
        echo "Fail to process one meta file: ${one_meta}"
        exit ${rcode}
    fi
    hdfs dfs -rm ${one_meta}
done
cd ..
rm -r -f ${tmp_dir}