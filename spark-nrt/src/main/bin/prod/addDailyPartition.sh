#!/usr/bin/env bash
set -x

export HADOOP_USER_NAME=chocolate

DEST_CLUSTER=${1}
TABLE_NAME=${2}

if [ "${DEST_CLUSTER}" == "rno" ]
then
    command_hive="/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive"
elif [ "${DEST_CLUSTER}" == "hercules" ]
then
    command_hive="/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive"
else
    echo "Wrong cluster!"
    exit 1
fi

retry_add=1
rcode_add=1
until [[ ${retry_add} -gt 3 ]]
do
    ${command_hive} -e "set hive.msck.path.validation=ignore; MSCK REPAIR TABLE ${TABLE_NAME}"
    rcode_add=$?
    if [ ${rcode_add} -eq 0 ]
    then
        break
    else
        echo "Failed to repair ${DEST_CLUSTER} ${TABLE_NAME}."
        retry_add=$(expr ${retry_add} + 1)
    fi
done
if [ ${rcode_add} -ne 0 ]
then
    echo -e "Failed to repair ${DEST_CLUSTER} ${TABLE_NAME}, please check!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "Failed to repair ${DEST_CLUSTER} ${TABLE_NAME}, please check!!!" -v DL-eBay-Chocolate-GC@ebay.com
    exit ${rcode_add}
else
    echo "Repair ${DEST_CLUSTER} ${TABLE_NAME} successfully."
    exit 0
fi