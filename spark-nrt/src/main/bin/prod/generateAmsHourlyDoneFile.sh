#!/usr/bin/env bash

DONE_FILE=$1
DONE_FILE_DIR=/apps/b_marketing_tracking/watch/

done_date=${DONE_FILE:22:8}
done_file_full_name=${DONE_FILE_DIR}${done_date}'/'${DONE_FILE}
local_done_date_file="/datashare/mkttracking/data/epn-nrt/local_done_date.txt"

#################################### Generate hourly done file on Apollo Rno ####################################
echo "=============== Start generating hourly done file on rno ==============="

HOST_NAME=`hostname -f`
kinit -kt /datashare/mkttracking/tools/keytab-tool/keytab/b_marketing_tracking.${HOST_NAME}.keytab  b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -e ${done_file}
done_file_exists=$?
if [ ${done_file_exists} -eq 0 ]
then
    echo "done file exists: ${done_file}"
    exit 0
fi

retry=1
rcode_rno=1
until [[ ${retry} -gt 3 ]]
do
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -touchz ${done_file_full_name}
    rcode_rno=$?
    if [ ${rcode_rno} -eq 0 ]
    then
         echo "Successfully generated done file on Apollo Rno: "${done_file_full_name}
         break
    else
         echo "Faild to generate done file on Apollo Rno: "${done_file_full_name}", retrying ${retry}"
         retry=`expr ${retry} + 1`
    fi
done


#################################### Generate hourly done file on Hercules ####################################
echo "=============== Start generating hourly done file on hercules ==============="

reno_done_file_full_name='viewfs://apollo-rno'${done_file_full_name}
hercules_done_file_full_name='hdfs://hercules'${done_file_full_name}

retry=1
rcode_rno=1
until [[ ${retry} -gt 3 ]]
do
    /datashare/mkttracking/tools/cake/bin/distcp_by_optimus.sh ${reno_done_file_full_name} ${hercules_done_file_full_name} epnnrt_done
    rcode_hercules=$?
    if [ ${rcode_hercules} -eq 0 ]
    then
         echo "Successfully generated done file on Hercules: ${done_file_full_name}"
         break
    else
         echo "Faild to generate done file on Apollo Rno: "${done_file_full_name}", retrying ${retry}"
         retry=`expr ${retry} + 1`
    fi
done


######################################### Save done time to local file #########################################
echo ${DONE_FILE:22:10} > ${local_done_date_file}