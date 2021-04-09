#!/usr/bin/env bash
# run job to format ePN nrt data
echo `date`
usage="Usage: epnnrt_click-scheduler_v2.sh"
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

##################### SPARK NRT Job Parameters ##################
SLC_WORK_DIR=hdfs://slickha/apps/tracking-events-workdir-v2
LVS_WORK_DIR=hdfs://elvisha/apps/tracking-events-workdir-v2
RESOURCE_DIR=/apps/tracking-resources-v2
FILTER_HOUR=1     # 1:00am
log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt-v2/scheduler_${log_dt}.log"

echo "LVS_WORK_DIR="${LVS_WORK_DIR} | tee -a ${log_file}
echo "SLC_WORK_DIR="${SLC_WORK_DIR} | tee -a ${log_file}

echo "RESOURCE_DIR="${RESOURCE_DIR} | tee -a ${log_file}
echo "FILTER_HOUR="${FILTER_HOUR} | tee -a ${log_file}

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
/datashare/mkttracking/jobs/tracking/epnnrt_v2/bin/prod/epnnrt_click_v2.sh ${SLC_WORK_DIR} ${LVS_WORK_DIR} ${RESOURCE_DIR} "0"