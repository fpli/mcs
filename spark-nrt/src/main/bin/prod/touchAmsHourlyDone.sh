#!/usr/bin/env bash

DONE_FILE=$1
LOCAL_DONE_DATE_FILE=$2

DONE_FILE_DIR=/apps/b_marketing_tracking/watch/

done_date=${DONE_FILE:22:8}
done_hour=${DONE_FILE:30:2}
done_file_full_dir=${DONE_FILE_DIR}${done_date}
done_file_full_name=${done_file_full_dir}'/'${DONE_FILE}


################################### Add partition to Hive on Reno and Hercules ###################################

if [ "${done_hour}" == "00" ]
then
    echo "===================== Add partition to Hive on Reno and Hercules ====================="
    
    /datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -e "set hive.msck.path.validation=ignore; MSCK repair table choco_ams_click;"
    /datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -e "set hive.msck.path.validation=ignore; MSCK repair table choco_ams_imprsn;"
    /datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -e "set hive.msck.path.validation=ignore; MSCK repair table im_tracking.choco_ams_click;"
    /datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -e "set hive.msck.path.validation=ignore; MSCK repair table im_tracking.choco_ams_imprsn;"
fi


######################################## Generate hourly done file on Hercules ########################################

echo "===================== Start generating hourly done file on hercules ====================="

## Create hourly done date dir if it doesn't exist
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -test -e ${done_file_full_dir}
done_dir_exists=$?
if [ ${done_dir_exists} -ne 0 ]
then
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
            echo "Successfully generated done file on Apollo Rno: "${done_file_full_name}
            break
        else
            echo "Faild to generate done file on Apollo Rno: "${done_file_full_name}", retrying ${retry}"
            retry=`expr ${retry} + 1`
        fi
    done
fi
if [ ${rcode} -ne 0 ]
    then
        echo -e "Fail to touch hourly done file: ${done_file_full_name} to Hercules!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "NRT Error!!!(Hourly done file to Hercules)" -v DL-eBay-Chocolate-GC@ebay.com
        exit ${rcode}
fi


############################################ Save done time to local file ############################################

echo "===================== Save done time to local file ====================="

echo ${DONE_FILE:22:10} > ${LOCAL_DONE_DATE_FILE}