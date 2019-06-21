#!/usr/bin/env bash

DONE_FILE=$1
DONE_FILE_DIR=/apps/b_marketing_tracking/watch/

done_date=${DONE_FILE:22:8}
done_file_full_name=${DONE_FILE_DIR}${done_date}'/'${DONE_FILE}
local_done_date_file="/datashare/mkttracking/chocolate/epn-nrt/local_done_date.txt"

#################################### Generate hourly done file on Apollo Rno ####################################
echo "Start generating hourly done file on rno"

HOST_NAME=`hostname -f`
kinit -kt /datashare/mkttracking/tools/keytab-tool/keytab/b_marketing_tracking.${HOST_NAME}.keytab  b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -e ${done_file}
done_file_exists=$?
if [ ${done_file_exists} -eq 0 ]
then
    echo "done file exists: ${done_file}"
    exit 0
fi

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -touchz ${done_file_full_name}
echo "Successfully generated done file on Apollo Rno: ${done_file_full_name}"


#################################### Generate hourly done file on Hercules ####################################
echo "Start generating hourly done file on hercules"

reno_done_file_full_name='viewfs://apollo-rno'${done_file_full_name}
hercules_done_file_full_name='hdfs://hercules'${done_file_full_name}

./distcp.sh ${reno_done_file_full_name} ${hercules_done_file_full_name} epnnrt_done
echo "Successfully generated done file on Hercules: ${done_file_full_name}"


######################################### Save done time to local file #########################################
echo ${DONE_FILE:22:10} | tee -a ${local_done_date_file}