#!/bin/bash

# read output meta files by channel and action, put data files to Apollo Rno

WORK_DIR=$1
CHANNEL=$2
ACTION=$3
META_FILE_SUFFIX=$4
RNO_WORK_DIR=$5
RNO_DATA_DIR=$6

rno_middle_dir=${RNO_WORK_DIR}/${CHANNEL}/${ACTION}
rno_output_dir=${RNO_DATA_DIR}/${CHANNEL}/${ACTION}

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
    echo "start put files size:"${files_size}

    data_files=`cat ${output_file} | tr "\n" " "`
    while read -r date_file; do
        file_date=`echo ${date_file} | cut -d" " -f1`
        data_file=`echo ${date_file} | cut -d" " -f2`
        data_file_name=$(basename "$data_file")
        rm -f data_file_name
        hdfs dfs -get ${data_file}

        dest_dir=${rno_output_dir}/${file_date}
        /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p ${dest_dir}

        command_1="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -put -f ${data_file_name} ${rno_middle_dir}/"
        command_2="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -rm -f ${dest_dir}/${data_file_name}"
        command_3="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mv ${rno_middle_dir}/${data_file_name} ${dest_dir}/"

        retry=1
        rcode=1
        until [[ ${retry} -gt 3 ]]
        do
            ${command_1} && ${command_2} && ${command_3}
            rcode=$?
            if [ ${rcode} -eq 0 ]
            then
                echo "success put to RNO: "${data_file_name}
                break
            else
                echo "Faild to put to RNO, retrying ${retry}"
                retry=`expr ${retry} + 1`
             fi
        done
        rm -f ${data_file_name}
        if [ ${rcode} -ne 0 ]
        then
            echo "Fail to put to RNO, please check!!!"
            exit ${rcode}
        fi
    done < "$output_file"
    rm -f ${output_file}
    echo "finish put files size:"${files_size}
}

export HADOOP_USER_NAME=chocolate
HOST_NAME=`hostname -f`
kinit -kt /datashare/mkttracking/tools/keytab-tool/keytab/b_marketing_tracking.${HOST_NAME}.keytab  b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM

meta_dir=${WORK_DIR}'/meta/'${CHANNEL}'/output/'${ACTION}
tmp_dir='tmp_put_to_rno_'${CHANNEL}'_'${ACTION}

mkdir -p ${tmp_dir}
cd ${tmp_dir}

all_meta_files=all_meta_files.txt
meta_suffix='\.'${META_FILE_SUFFIX}'$'
hdfs dfs -ls ${meta_dir} | grep -v "^$" | awk '{print $NF}' | grep ${meta_suffix} > ${all_meta_files}

files_size=`cat ${all_meta_files} | wc -l`
echo "start process meta files size:"${files_size}

all_files=`cat ${all_meta_files} | tr "\n" " "`
for one_meta in ${all_files}
do
    process_one_meta ${one_meta}
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