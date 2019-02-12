#!/usr/bin/env bash
export HADOOP_USER_NAME=hdfs

usage="Usage: DistcpImkToApollo.sh [srcDir] [destDir] [archiveDir]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

SRC_DIR=$1
DEST_DIR=$2
ARCHIVE_DIR=$3

DIFF=0
DATE=`date --date=$DIFF" days ago" +%Y%m%d`
dt="dt="${DATE}

#date files path in chocolate hdfs
CHOCOLATE_HDFS_DATA_PATH=${SRC_DIR}
#archive date files path in chocolate hdfs
CHOCOLATE_HDFS_DATA_ACHIVE_PATH=${ARCHIVE_DIR}
#apollo path
APOLLO_PATH=${DEST_DIR}/${dt}

#log all copied files name
LOG_FILE=/tmp/rover_imk_apollo_history.log

#create archive daily folder if not exists
archive_folder=${CHOCOLATE_HDFS_DATA_ACHIVE_PATH}/${dt}
hdfs dfs -mkdir -p ${archive_folder}

#create apollo daily folder if not exists
kinit -kt b_marketing_tracking_APD.keytab b_marketing_tracking@APD.EBAY.COM
/apache/hadoop/bin/hdfs dfs -mkdir -p ${APOLLO_PATH}

#save to tmp file, make the tmp file unique
tmp_file_name=rover_imk_apollo_tmp.txt
hdfs dfs -ls -R ${CHOCOLATE_HDFS_DATA_PATH} | grep -v "^$" | awk '{print $NF}' | grep "chocolate-" > ${tmp_file_name}

files_size=`cat ${tmp_file_name} | wc -l`
echo "start copy files size:"${files_size}

all_files=`cat ${tmp_file_name} | tr "\n" " "`
if [ ${files_size} -gt 1 ]; then
    #copy files to apollo,
    hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_APD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking@APD.EBAY.COM -targetkeytab b_marketing_tracking_APD.keytab -skipcrccheck -update ${all_files} ${APOLLO_PATH}
    if [ $? -ne 0 ]; then
	    echo "copy to apollo failed, exit"
	    exit 1
    fi
    echo "finish copy to apollo files size:"${files_size}

    #move date files to archive folder
    for one_file in ${all_files}
    do
        hdfs dfs -mv ${one_file} ${archive_folder}
    done
    echo "finish archive files size:"${files_size}

    #log to log file
    cat ${tmp_file_name} >> ${LOG_FILE}
    rm ${tmp_file_name}
else
    echo "No more than 1 file need to copy"
fi

