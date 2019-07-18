#!/usr/bin/env bash

DONE_TIME=$1
LOCAL_DONE_DATE_FILE=$2
ACTION=$3
DONE_CLUSTER=$4

DONE_FILE_DIR=/apps/b_marketing_tracking/watch/

done_date=${DONE_TIME:0:8}
done_hour=${DONE_TIME:8:2}
done_file_full_dir=${DONE_FILE_DIR}${done_date}

if [ "${ACTION}" == "click" ]
then
    done_file_full_name="${done_file_full_dir}/ams_click_hourly.done.${DONE_TIME}00000000"
elif [ "${ACTION}" == "imp" ]
then
    done_file_full_name="${done_file_full_dir}/ams_imprsn_hourly.done.${DONE_TIME}00000000"
else
    echo "Wrong action to touch hourly done!"
    exit 1
fi

if [ "${DONE_CLUSTER}" == "reno" ]
then
    command_hive="/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive"
    command_hadoop="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs"
    table_click=choco_data.ams_click
    table_imp=choco_data.ams_imprsn
elif [ "${DONE_CLUSTER}" == "hercules" ]
then
    command_hive="/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive"
    command_hadoop="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs"
    table_click=im_tracking.ams_click
    table_imp=im_tracking.ams_imprsn
else
    echo "Wrong cluster to touch hourly done!"
    exit 1
fi


################################# Add partition to Hive on Reno or Hercules once a day #################################

if [ "${done_hour}" == "00" ]
then
    echo "======================== Add partition to Hive on Reno or Hercules ========================"

    retry_repair=1
    rcode_repair=1
    until [[ ${retry_repair} -gt 3 ]]
    do
        ${command_hive} -e "set hive.msck.path.validation=ignore; MSCK repair table ${table_click};" &&
        ${command_hive} -e "set hive.msck.path.validation=ignore; MSCK repair table ${table_imp};"
        rcode_repair=$?
        if [ ${rcode_repair} -eq 0 ]
        then
            break
        else
            echo "Failed to add today's partition to hive."
            retry_repair=`expr ${retry_repair} + 1`
        fi
    done
    if [ ${rcode_repair} -ne 0 ]
    then
        echo -e "Failed to add today's partition on hive!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in adding hive partition!!!" -v DL-eBay-Chocolate-GC@ebay.com
        exit ${rcode_repair}
    fi
fi


###################################### Touch hourly done file on ${DONE_CLUSTER} ######################################

echo "==================== Start touching hourly done file on ${DONE_CLUSTER} ===================="

## Create hourly done date dir if it doesn't exist
${command_hadoop} -test -e ${done_file_full_dir}
done_dir_exists=$?
if [ ${done_dir_exists} -ne 0 ]
then
    echo "Create ${DONE_CLUSTER} folder ${done_file_full_dir}"
    ${command_hadoop} -mkdir ${done_file_full_dir}
fi

## Touch done file, retry 3 times
rcode=1
${command_hadoop} -test -e ${done_file_full_name}
done_file_exists=$?
if [ ${done_file_exists} -eq 0 ]
then
    echo "done file exists: ${done_file}"
    rcode=0
else
    retry=1
    until [[ ${retry} -gt 3 ]]
    do
        ${command_hadoop} -touchz ${done_file_full_name}
        rcode=$?
        if [ ${rcode} -eq 0 ]
        then
            echo "Successfully touch done file on ${DONE_CLUSTER}: "${done_file_full_name}
            break
        else
            echo "Failed to touch done file on ${DONE_CLUSTER}: "${done_file_full_name}", retrying ${retry}"
            retry=`expr ${retry} + 1`
        fi
    done
fi
if [ ${rcode} -ne 0 ]
    then
        echo -e "Failed to touch hourly done file: ${done_file_full_name} on ${DONE_CLUSTER}!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in touching hourly done file on ${DONE_CLUSTER}!!!" -v DL-eBay-Chocolate-GC@ebay.com
        exit ${rcode}
fi


############################################ Save done time to local file ############################################

echo "=============================== Save done time to local file ==============================="

echo ${DONE_TIME} > ${LOCAL_DONE_DATE_FILE}