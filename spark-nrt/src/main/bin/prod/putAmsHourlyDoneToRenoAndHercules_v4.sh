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
RENO_MIN_TS_FILE_CLICK=/apps/b_marketing_tracking/chocolate/epnnrt_v2/min_ts_rno_click.txt
RENO_MIN_TS_FILE_IMP=/apps/b_marketing_tracking/chocolate/epnnrt_v2/min_ts_rno_imp.txt

if [ ${TYPE} == "click" ]
then
  echo "================ reno click touch hourly done file ================"
  ./checkAndTouchAmsHourlyDone.sh ${WORK_DIR} ${CHANNEL} ${USAGE_CLICK} ${RENO_META_SUFFIX} ${RENO_LOCAL_DONE_DATE_FILE_CLICK} ${RENO_MIN_TS_FILE_CLICK} click

  echo "================ hercules click touch hourly done file ================"
elif [ ${TYPE} == "imp"  ]; then
  echo "================ reno imp touch hourly done file ================"
  ./checkAndTouchAmsHourlyDone.sh ${WORK_DIR} ${CHANNEL} ${USAGE_IMP} ${RENO_META_SUFFIX} ${RENO_LOCAL_DONE_DATE_FILE_IMP} ${RENO_MIN_TS_FILE_IMP} imp
else
  echo $usage
  exit 1
fi