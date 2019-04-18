#!/bin/bash

usage="Usage: imkCopiedToRNO.sh [HDFS_PATH_1] [HDFS_PATH_2] ... [[HDFS_PATH_N]"

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo ${usage}
  exit 1
fi

for i in $@; do
    HDFS_PATH=$i
    tmp_file=imk_copied_to_reno.txt
    hdfs dfs -ls -R ${HDFS_PATH} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" > ${tmp_file}
    files_size=`cat ${tmp_file} | wc -l`
    rm -f ${tmp_file}

    if [ ${files_size} -gt 4 ];
    then
        echo "too many data files remaining in ${HDFS_PATH}: ${files_size}"
        exit 2
    fi
done

