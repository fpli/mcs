#!/usr/bin/env bash
# run job to format ePN nrt data
echo `date`
usage="Usage: epnnrt_click_automation_test-scheduler.sh"
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

##################### SPARK NRT Job Parameters ##################
INPUT_WORK_DIR_OLD=hdfs://slickha/apps/tracking-events-workdir-old-test
OUTPUT_WORK_DIR_OLD=hdfs://slickha/apps/tracking-events-workdir-old-test
INPUT_WORK_DIR_NEW=hdfs://slickha/apps/tracking-events-workdir-new-test
OUTPUT_WORK_DIR_NEW=hdfs://slickha/apps/tracking-events-workdir-new-test

RESOURCE_DIR=/apps/tracking-resources-test
FILTER_HOUR=1     # 1:00am
log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt/scheduler_${log_dt}.log"

##################### Generate FILTER_TIME ##################
DT=$(date +%Y-%m-%d)
HOUR=$(date +%_H)

if [[ $# -eq 1 ]]; then
  DT_HOUR=$(date +%Y-%m-%d' '$1:00:00)
elif [[ ${HOUR} -ge ${FILTER_HOUR} ]]; then
  DT_HOUR=$(date +%Y-%m-%d' '00:00:00)
else
  DT_HOUR=$'\0'
fi

if [[ ! -z "${DT_HOUR}" ]]; then
  FILTER_TIME=$(date +%s -d "$DT_HOUR")000
else
  FILTER_TIME=0
fi

echo "DT_HOUR="${DT_HOUR} | tee -a ${log_file}
echo "FILTER_TIME="${FILTER_TIME} | tee -a ${log_file}

##################### Spark Submit ##################
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
/datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/auto_create_meta_by_date_c3.sh "2021-04-09"
/datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/epnnrt_click_v2.sh ${INPUT_WORK_DIR_OLD} ${OUTPUT_WORK_DIR_OLD} ${RESOURCE_DIR} "0"
/datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/epnnrt_click_v2.sh ${INPUT_WORK_DIR_NEW} ${OUTPUT_WORK_DIR_NEW} ${RESOURCE_DIR} "0"