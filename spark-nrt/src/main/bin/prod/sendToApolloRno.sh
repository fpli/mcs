#!/usr/bin/env bash
usage="Usage: sendToApolloRno.sh"

log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)

HDP=$1
INPUT_FILE=$2
log_file=$3

echo "HDP=$HDP"
echo "INPUT_FILE=$INPUT_FILE"
echo "log_file=$log_file"

/datashare/mkttracking/tools/keytab-tool/kinit/kinit_byhost.sh

echo `date`"=====================================================Apollo_rno -- LoadData started======================================================" | tee -a ${log_file}
echo `date`"------ Apollo_rno -- LoadData started~~~" | tee -a ${log_file}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -ls ${HDP} > folder.txt

epnFiles=`wc -l folder.txt`
epnCnt=$(awk -F' '  '{print $1}'<<<${epnFiles})

if [[ ${epnCnt} -eq 0 ]]; then
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -mkdir -p ${HDP}
fi

#max 3 times copy data to reno
retry=1
rcode=1

until [[ ${retry} -gt 3 ]]
do
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -copyFromLocal ${INPUT_FILE} ${HDP}
    rcode=$?
    if [ ${rcode} -eq 0 ]
    then
        break
    else
        echo "Faild to upload NRT Data to Reno, retrying ${retry}"
        retry=`expr ${retry} + 1`
    fi
done
if [ ${rcode} -ne 0 ]; then
    echo "Fail to upload EPN NRT Data to Reno, please check!!!" | mail -s "Send EPN NRT To Reno ERROR!!!!" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit ${rcode}
fi

echo `date`"=====================================================Apollo_rno -- LoadData Ended======================================================" | tee -a ${log_file}