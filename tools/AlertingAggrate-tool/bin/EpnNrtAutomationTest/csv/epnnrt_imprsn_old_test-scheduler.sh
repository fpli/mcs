#!/usr/bin/env bash
# run job to format ePN nrt data
echo `date`
usage="Usage: epnnrt_imprsn_old_test-scheduler.sh"
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

##################### SPARK NRT Job Parameters ##################
WORK_DIR=hdfs://slickha/apps/tracking-events-workdir-old-test
RESOURCE_DIR=/apps/tracking-resources-old-test

OUTPUT_DIR=hdfs://slickha/apps/epn-nrt-old-test

DATE=`date -d '5 days ago' +%Y-%m-%d`

FILTER_HOUR=1     # 1:00am
log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt-old-test/scheduler_${log_dt}.log"


##################### Generate FILTER_TIME ##################
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
export HADOOP_USER_NAME=hdfs
echo $HADOOP_USER_NAME
hdfs dfs -rm -r /apps/epn-nrt-old-test/impression/*
/datashare/mkttracking/jobs/tracking/epnnrt_old_test/bin/prod/epnnrt_imprsn_automation_test_csv.sh ${WORK_DIR}  ${RESOURCE_DIR} "0" ${OUTPUT_DIR}
/datashare/mkttracking/jobs/tracking/epnnrt_old_test/bin/prod/distcpAmsToRenoForAutomation.sh /apps/epn-nrt-old-test/impression /apps/b_marketing_tracking/chocolate/epnnrt-old-test/imp imp ${DATE}
