#!/bin/bash

# Put imk hourly done from chocolate hadoop to Apollo RNO and Hercules.
# Output:   Apollo RNO
#           /apps/b_marketing_tracking/watch
#           Hercules
#           /apps/b_marketing_tracking/watch
# Schedule: /3 * ? * *
# case：
#./putAmsHourlyDoneToRenoAndHercules_v2.sh click
#./putAmsHourlyDoneToRenoAndHercules_v2.sh imp

set -x

usage="Usage: putAmsHourlyDoneToRenoAndHercules_v2.sh [type]"

if [ $# -lt 1 ]; then
  echo $usage
  exit 1
fi

TYPE=$1

WORK_DIR=viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir
CHANNEL=EPN
USAGE_CLICK=epnnrt_scp_click
USAGE_IMP=epnnrt_scp_imp

RENO_META_SUFFIX=.epnnrt_reno
RENO_LOCAL_DONE_DATE_FILE_CLICK=/datashare/mkttracking/data/epn-nrt-v2/local_done_date_rno_click.txt
RENO_LOCAL_DONE_DATE_FILE_IMP=/datashare/mkttracking/data/epn-nrt-v2/local_done_date_rno_imp.txt
RENO_MIN_TS_FILE_CLICK=/user/b_marketing_tracking/chocolate/epnnrt_v2/min_ts_rno_click.txt
RENO_MIN_TS_FILE_IMP=/user/b_marketing_tracking/chocolate/epnnrt_v2/min_ts_rno_imp.txt

function get_current_done(){
    last_done=`cat $1`
    last_ts=`date -d "${last_done:0:8} ${last_done:8}" +%s`
    let current_ts=${last_ts}+3600
    current_done=`date -d @${current_ts} "+%Y%m%d%H"`
    echo ${current_done}
}

if [ ${TYPE} == "click" ]
then
  echo "================ reno click touch hourly done file ================"
  ./checkAmsHourlyDone_v3.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${RENO_META_SUFFIX} ${RENO_LOCAL_DONE_DATE_FILE_CLICK} ${RENO_MIN_TS_FILE_CLICK}
  rcode_check_click=$?
  if [ ${rcode_check_click} -eq 1 ];
  then
      current_done_click=$(get_current_done ${RENO_LOCAL_DONE_DATE_FILE_CLICK})
      echo "=================== Start touching reno click hourly done file ==================="
      ./touchAmsHourlyDone_v3.sh ${current_done_click} ${RENO_LOCAL_DONE_DATE_FILE_CLICK} click reno
      ./touchAmsHourlyDone_v3.sh ${current_done_click} ${RENO_LOCAL_DONE_DATE_FILE_CLICK} click hercules
  fi

  echo "================ hercules click touch hourly done file ================"
elif [ ${TYPE} == "imp"  ]; then
  echo "================ reno imp touch hourly done file ================"
  ./checkAmsHourlyDone_v3.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${RENO_META_SUFFIX} ${RENO_LOCAL_DONE_DATE_FILE_IMP} ${RENO_MIN_TS_FILE_IMP}
  rcode_check_imp=$?
  if [ ${rcode_check_imp} -eq 1 ];
  then
      current_done_imp=$(get_current_done ${RENO_LOCAL_DONE_DATE_FILE_IMP})
      echo "=================== Start touching reno imp hourly done file ==================="
      ./touchAmsHourlyDone_v3.sh ${current_done_imp} ${RENO_LOCAL_DONE_DATE_FILE_IMP} imp reno
      ./touchAmsHourlyDone_v3.sh ${current_done_imp} ${RENO_LOCAL_DONE_DATE_FILE_IMP} imp hercules
  fi
else
  echo $usage
  exit 1
fi