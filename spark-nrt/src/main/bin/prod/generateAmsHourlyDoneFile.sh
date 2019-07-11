#!/usr/bin/env bash

DONE_FILE=$1
LOCAL_DONE_DATE_FILE=$2

DONE_FILE_DIR=/apps/b_marketing_tracking/watch/

done_date=${DONE_FILE:22:8}
done_file_full_dir=${DONE_FILE_DIR}${done_date}
done_file_full_name=${done_file_full_dir}'/'${DONE_FILE}

#################################### Generate hourly done file on Apollo Rno ####################################

echo "=============== Start generating hourly done file on rno ==============="

HOST_NAME=`hostname -f`
kinit -kt /datashare/mkttracking/tools/keytab-tool/keytab/b_marketing_tracking.${HOST_NAME}.keytab  b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM

## Create hourly done date dir if it doesn't exist
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -e ${done_file_full_dir}
done_dir_exists=$?
if [ ${done_dir_exists} -ne 0 ]
then
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir ${done_file_full_dir}
fi

## Touch done file, retry 3 times
rcode_rno=1
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -test -e ${done_file_full_name}
done_file_exists=$?
if [ ${done_file_exists} -eq 0 ]
then
    echo "done file exists: ${done_file}"
    rcode_rno=0
else
    retry=1
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
fi
if [ ${rcode_rno} -ne 0 ]
    then
        echo -e "Fail to send hourly done file: ${done_file_full_name} to Apollo Reno!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "NRT Error!!!(Hourly done file to Apollo Reno)" -v DL-eBay-Chocolate-GC@ebay.com
        exit ${rcode_rno}
fi


#################################### Generate hourly done file on Hercules ####################################
: << !
echo "=============== Start generating hourly done file on hercules ==============="

reno_done_file_full_name='viewfs://apollo-rno'${done_file_full_name}
hercules_done_file_full_dir='hdfs://hercules'${done_file_full_dir}

rcode_hercules=1
retry=1
until [[ ${retry} -gt 3 ]]
do
    /datashare/mkttracking/tools/cake/bin/distcp_by_optimus.sh ${reno_done_file_full_name} ${hercules_done_file_full_dir} epnnrt_done
    rcode_hercules=$?
    if [ ${rcode_hercules} -eq 0 ]
    then
         echo "Successfully generated done file on Hercules: ${done_file_full_name}"
         break
    else
         echo "Faild to generate done file on Hercules: "${done_file_full_name}", retrying ${retry}"
         retry=`expr ${retry} + 1`
    fi
done
if [ ${rcode_hercules} -ne 0 ]
    then
        echo -e "Fail to send hourly done file: ${done_file_full_name} to Hercules!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "NRT Error!!!(Hourly done file to Hercules)" -v DL-eBay-Chocolate-GC@ebay.com
        exit ${rcode_hercules}
fi
!

######################################### Save done time to local file #########################################

echo ${DONE_FILE:22:10} > ${LOCAL_DONE_DATE_FILE}