#!/bin/bash
set -x
# kill long run job in Apollo and Hercules
# CLUSTER : APOLLO OR HERCULES
# JOB : class com.ebay.traffic.chocolate.sparknrt.hourlyDone.UTPImkHourlyDoneJob
# MAXTIME : unit minute

if [ $# -lt 3 ]; then
  echo $usage
  exit 1
fi

CLUSTER=$1
JOB=$2
MAXTIME=$3

if [ "${CLUSTER}" == "APOLLO" ]
then
  YARN="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/yarn "
elif [ "${CLUSTER}" == "HERCULES" ]
then
  YARN="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/yarn "
else
    echo "Wrong cluster!"
    exit 1
fi

application_id=$($YARN application -list | grep ${JOB} | awk '{ print $1; }')
start_time=$($YARN application -status ${application_id} | grep 'Start-Time' | sed 's/[^0-9]*//g')

echo "application_id: $application_id"
echo "start_time: $start_time"

if [[ -z $application_id ||  -z $start_time ]]
then
    echo "The job's apilication was not found";
    exit 0;
fi

ts=$(date +%s)
diff=$((($ts*1000-$start_time)/1000/60))
echo "ts: $ts"
echo "diff: $diff"

if [ $diff -gt $MAXTIME ]; then
    $YARN application -kill $application_id
    echo -e "Kill long run job application: ${JOB} in ${CLUSTER}, execute time ${diff} m!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[LONG RUN] Kill long run job application: ${JOB}!!!" -v DL-eBay-Chocolate-GC@ebay.com
fi