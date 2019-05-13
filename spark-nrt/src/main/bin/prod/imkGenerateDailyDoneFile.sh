#!/bin/bash

# Generate hadoop done file in ApolloRNO
# Input:    /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event
#           /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl
# Output:   /apps/b_marketing_tracking/imk_tracking/daily_done_files/imk_rvr_trckng_event.date
#           /apps/b_marketing_tracking/imk_tracking/daily_done_files/imk_rvr_trckng_event.date
# Schedule: 23 1-23 ? * *
#           21 1-23 ? * *

usage="Usage: imkGenerateDailyDoneFile.sh [DONE_FILE_RNO_PATH] [IMK_RNO_PATH] [TABLE_NAME]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo ${usage}
  exit 1
fi

FILE_SIZE_THRESHOLD=18

DONE_FILE_RNO_PATH=$1
IMK_RNO_PATH=$2
TABLE_NAME=$3

HOST_NAME=`hostname -f`
kinit -kt /datashare/mkttracking/tools/keytab-tool/keytab/b_marketing_tracking.${HOST_NAME}.keytab  b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM

dt=$(date +%Y%m%d)
dts="dt=${dt}"

done_file=${DONE_FILE_RNO_PATH}/${TABLE_NAME}_$(date -d '-1 day' '+%Y%m%d').done
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -e ${done_file}
done_file_exists=$?
if [ ${done_file_exists} -eq 0 ]
then
    echo "done file exists: ${done_file}"
    exit 0
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





