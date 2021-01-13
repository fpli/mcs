#!/bin/bash
# distcp imk new data from Apollo to Hercules
# run on SLC server slcchocolatepits-1242736
# Input: beginDate ---- 20201009
#        endDate ----- 20201010
set -x
usage="Usage: distcpImkApolloFileToHercules.sh [beginDate] [endDate]"

if [ $# -lt 2 ]; then
  echo $usage
  exit 1
fi

BEGIN_DATE=${1}
END_DATE=${2}

echo "BEGIN_DATE:$BEGIN_DATE";
echo "END_DATE:$END_DATE";

if [[ -z $BEGIN_DATE ||  -z $END_DATE || $END_DATE < $BEGIN_DATE ]]
then
    echo $usage;
    exit 1;
fi

current_date=$BEGIN_DATE;
echo "current_date:$current_date";

LOCAL_PATH='/datashare/mkttracking/jobs/imkReformate'

IMK_NEW_PATH_APOLLO='/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_v2'
IMK_NEW_PATH_HERCULES='/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_v2/snapshot'

ENV_PATH='/datashare/mkttracking/tools/cake'
JOB_NAME='DistcpImkV2ToRenoAndHerculesBatchJob'
echo "ENV_PATH:${ENV_PATH}"
echo "JOB_NAME:${JOB_NAME}"

cd ${LOCAL_PATH}
pwd
while [[ $current_date -le $END_DATE ]]; do
    echo "current_date:$current_date";
    echo "begin deal with data in $current_date";
    dt=${current_date:0:4}-${current_date:4:2}-${current_date:6}
    echo "dt:$dt";
    RNO_PATH="hdfs://apollo-rno${IMK_NEW_PATH_APOLLO}/dt=${dt}"
    HERCULES_PATH="hdfs://hercules${IMK_NEW_PATH_HERCULES}"
    /datashare/mkttracking/tools/cake/bin/datamove_apollo_rno_to_hercules.sh ${RNO_PATH} ${HERCULES_PATH} ${JOB_NAME} ${ENV_PATH}
    echo "end deal with data in $current_date";
    current_date=$(date -d"${current_date} 1 days" +"%Y%m%d");
done