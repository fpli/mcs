#!/usr/bin/env bash

DONE_TIME=$1
LOCAL_DONE_DATE_FILE=$2
ACTION=$3

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
fi


################################ Add partition to Hive on Reno and Hercules once a day ################################

if [ "${done_hour}" == "00" ]
then
    echo "======================== Add partition to Hive on Reno and Hercules ========================"

    command_1="/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -e "set hive.msck.path.validation=ignore; MSCK repair table choco_data.ams_click;""
    command_2="/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -e "set hive.msck.path.validation=ignore; MSCK repair table choco_data.ams_imprsn;""
    command_3="/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -e "set hive.msck.path.validation=ignore; MSCK repair table im_tracking.ams_click;""
    command_4="/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -e "set hive.msck.path.validation=ignore; MSCK repair table im_tracking.ams_imprsn;""

    retry_repair=1
    rcode_repair=1
    until [[ ${retry_repair} -gt 3 ]]
    do
        ${command_1} && ${command_2} && ${command_3} && ${command_4}
        rcode_repair=$?
        if [ ${rcode_repair} -eq 0 ]
        then
            break
        else
            echo "Failed to add today's partition to hive."
            retry=`expr ${retry_repair} + 1`
        fi
    done
    if [ ${rcode_repair} -ne 0 ]
    then
        echo -e "Failed to add today's partition on hive!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in adding hive partition!!!" -v DL-eBay-Chocolate-GC@ebay.com
        exit ${rcode_repair}
    fi

fi


######################################### Touch hourly done file on Hercules #########################################

echo "======================= Start touching hourly done file on hercules ======================="

## Create hourly done date dir if it doesn't exist
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -test -e ${done_file_full_dir}
done_dir_exists=$?
if [ ${done_dir_exists} -ne 0 ]
then
    echo "Create hercules folder ${done_file_full_dir}"
    /datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -mkdir ${done_file_full_dir}
fi

## Touch done file, retry 3 times
rcode=1
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -test -e ${done_file_full_name}
done_file_exists=$?
if [ ${done_file_exists} -eq 0 ]
then
    echo "done file exists: ${done_file}"
    rcode=0
else
    retry=1
    until [[ ${retry} -gt 3 ]]
    do
        /datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -touchz ${done_file_full_name}
        rcode=$?
        if [ ${rcode} -eq 0 ]
        then
            echo "Successfully touch done file on Hercules: "${done_file_full_name}
            break
        else
            echo "Faild to touch done file on Hercules: "${done_file_full_name}", retrying ${retry}"
            retry=`expr ${retry} + 1`
        fi
    done
fi
if [ ${rcode} -ne 0 ]
    then
        echo -e "Failed to touch hourly done file: ${done_file_full_name} on Hercules!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in touching hourly done file on Hercules!!!" -v DL-eBay-Chocolate-GC@ebay.com
        exit ${rcode}
fi


############################################ Save done time to local file ############################################

echo "=============================== Save done time to local file ==============================="

echo ${DONE_TIME} > ${LOCAL_DONE_DATE_FILE}