#!/bin/bash

# Generate hadoop done file in ApolloRNO
# Input:    Apollo RNO
#           /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event
#           /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl
# Output:   Apollo RNO
#           /apps/b_marketing_tracking/imk_tracking/daily_done_files/imk_rvr_trckng_event.date
#           /apps/b_marketing_tracking/imk_tracking/daily_done_files/imk_rvr_trckng_event.date
# Schedule: 23 1-23 ? * *
#           21 1-23 ? * *

usage="Usage: imkGenerateDailyDoneFile.sh [DONE_FILE_RNO_PATH] [HOURLY_DONE_FILE_RNO_PATH] [TABLE_NAME]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo ${usage}
  exit 1
fi

DONE_FILE_RNO_PATH=$1
HOURLY_DONE_FILE_RNO_PATH=$2
TABLE_NAME=$3

dt=$(date -d '-1 day' '+%Y%m%d')

done_file=${DONE_FILE_RNO_PATH}/${TABLE_NAME}_${dt}.done
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -e ${done_file}
done_file_exists=$?
if [ ${done_file_exists} -eq 0 ]
then
    echo "done file exists: ${done_file}"
    exit 0
fi

hourly_done_file=${HOURLY_DONE_FILE_RNO_PATH}/${dt}/imk_rvr_trckng_event_hourly.done.${dt}2300000000
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -e ${hourly_done_file}
echo $hourly_done_file
hourly_done_file_exists=$?
if [ ${hourly_done_file_exists} -eq 0 ]
then
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -touchz ${done_file}
    echo "generated done file: ${done_file}"
else
    echo "${TABLE_NAME} done file delay!!!"
    exit 1
fi
tmp_file=imk_daily_done_check_${TABLE_NAME}.txt
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls -R ${IMK_RNO_PATH}/${dts} | grep -v "^$" | awk '{print $NF}' | grep "chocolate_" > ${tmp_file}
files_size=`cat ${tmp_file} | wc -l`
rm -f ${tmp_file}

if [ ${files_size} -gt ${FILE_SIZE_THRESHOLD} ];
then
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -touchz ${done_file}
    echo "generated done file: ${done_file}"
else
    echo "${TABLE_NAME} done file delay!!!"
    exit 1
fi