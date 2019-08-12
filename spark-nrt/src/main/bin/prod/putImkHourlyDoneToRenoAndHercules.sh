#!/bin/bash

# Put imk hourly done from chocolate hadoop to Apollo RNO and Hercules.
# Input:    SLC Hadoop
#           /apps/tracking-events/watch
# Output:   Apollo RNO
#           /apps/b_marketing_tracking/watch
#           Hercules
#           /apps/b_marketing_tracking/watch
# Schedule: /3 * ? * *

set -x

usage="Usage: putImkHourlyDoneToRenoAndHercules.sh [srcDir] [localTmpDir] [renoDestDir] [herculesDestDir]"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

export HADOOP_USER_NAME=chocolate

SRC_DIR=$1
LOCAL_TMP_DIR=$2
RENO_DEST_DIR=$3
HERCULES_DEST_DIR=$4

cd ${LOCAL_TMP_DIR}

dt_hour=$(date -d '1 hour ago' +%Y%m%d%H)
dt=${dt_hour:0:8}
done_file=${RENO_DEST_DIR}/${dt}/imk_rvr_trckng_event_hourly.done.${dt_hour}00000000

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -e ${done_file}

done_file_exists=$?
if [ ${done_file_exists} -eq 0 ]; then
    echo "done file exists: ${done_file}"
    exit 0
fi

today=${dt}
yesterday=$(date --date="${today} -1days" +%Y%m%d)

#get all done files in last two days
tmp_done_file=imk_to_reno_and_hercules_touched_done.txt
rm -f ${tmp_done_file}

hdfs dfs -ls -R ${SRC_DIR}/${yesterday} | grep -v "^$" | awk '{print $NF}' > ${tmp_done_file}
hdfs dfs -ls -R ${SRC_DIR}/${today} | grep -v "^$" | awk '{print $NF}' >> ${tmp_done_file}

already_touched_done_size=`cat ${tmp_done_file} | wc -l`
echo "already touched done files size:${already_touched_done_size}"

already_touched_done_files=`cat ${tmp_done_file} | tr "\n" " "`

# get all copied done files in last two days
tmp_copied_done_file=imk_to_reno_and_hercules_copied_done.txt
rm -f ${tmp_copied_done_file}

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls -R ${RENO_DEST_DIR}/${yesterday} | grep -v "^$" | awk '{{n=split ($NF,array,/\//);print array[n]}}' > ${tmp_copied_done_file}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls -R ${RENO_DEST_DIR}/${today} | grep -v "^$" | awk '{{n=split ($NF,array,/\//);print array[n]}}' >> ${tmp_copied_done_file}

already_copied_done_size=`cat ${tmp_copied_done_file} | wc -l`
echo "already copied done files size:${already_copied_done_size}"

already_copied_done_files=`cat ${tmp_copied_done_file} | tr "\n" " "`

for one_file in ${already_touched_done_files}
do
#   eg. imk_rvr_trckng_event_hourly.done.201904251100000000
    file_name=$(basename "$one_file")

    already_copied=1
    for VAR in ${already_copied_done_files} ; do
      if [[ "${VAR}" = ${file_name} ]]; then
        already_copied=0
        break;
      fi
    done

    if [[ "${already_copied}" -eq 0 ]] ;then
        continue
    fi

    echo "start copy file:"${file_name}

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
    renoDestFolder=${RENO_DEST_DIR}/${date}
    herculesDestFolder=${HERCULES_DEST_DIR}/${date}
#    create dest folder if not exists
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p ${renoDestFolder}
    /datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -mkdir -p ${herculesDestFolder}
#   check if done file exists
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -e ${renoDestFolder}/${file_name}
    fileExist=$?
    if [[ ${fileExist} -ne 0 ]]; then
#       max 3 times put done file to reno and hercules
        retry=1
        rcode=1
        until [[ ${retry} -gt 3 ]]
        do
            command_1="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -put -f ${file_name} ${renoDestFolder}/"
            command_2="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -put -f ${file_name} ${herculesDestFolder}/"
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
        echo "${file_name} done file existed"
    fi
#    remove local done file
    rm -f ${file_name}
    echo "finish copy file:"${file_name}
done
rm -f ${tmp_done_file}
rm -f ${tmp_copied_done_file}
echo "finish copy files size:"${already_touched_done_size}
