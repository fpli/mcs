#!/usr/bin/env bash

DT=$(date -d "1 day ago" +%Y-%m-%d )

echo "DT=${DT}"

export HADOOP_USER_NAME=chocolate

retry_add=1
rcode_add=1
until [[ ${retry_add} -gt 3 ]]
do
    /datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -e "set hive.msck.path.validation=ignore; ALTER TABLE im_tracking.tracking_event ADD IF NOT EXISTS PARTITION (dt='${DT}')"
    rcode_add=$?
    if [ ${rcode_add} -eq 0 ]
    then
        break
    else
        echo "Failed to add ${DT} partition to hive."
        retry_add=$(expr ${retry_add} + 1)
    fi
done
if [ ${rcode_add} -ne 0 ]
then
    echo -e "Failed to add ${DT} partition on hive, please check!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "TRACKING EVENT HERCULES ${DT} delayed!!!" -v DL-eBay-Chocolate-GC@ebay.com
    exit ${rcode_add}
else
    echo "Add ${DT} partition on hive successfully"
    exit 0
fi