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

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -copyFromLocal ${INPUT_FILE} ${HDP}
echo `date`"=====================================================Apollo_rno -- LoadData Ended======================================================" | tee -a ${log_file}