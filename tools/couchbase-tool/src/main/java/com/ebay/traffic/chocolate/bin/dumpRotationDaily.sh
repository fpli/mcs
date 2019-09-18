#!/bin/bash
# run job to pull rotation data from couchbase to Rno
echo `date`
usage="Usage: dumpRotationToRno.sh"

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

DT=$(date +%Y-%m-%d -d "`date` - 1 day")
START_DT_HOUR=$(date +%Y-%m-%d' '00:00:00 -d "`date` - 1 day")
END_DT_HOUR=$(date +%Y-%m-%d' '00:20:00 -d "`date`")

START_TIME=$(date +%s -d "$START_DT_HOUR")000
END_TIME=$(date +%s -d "$END_DT_HOUR")000

ROTATION_CONFIG_FILE=${bin}/../conf/
OUTPUT_PATH=${bin}/../output/dt=${DT}/

#log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S -d "$DT_HOUR")
#log_file="/datashare/mkttracking/logs/rotation/teradata/${log_dt}.log"
log_file="/datashare/mkttracking/jobs/tdmoveoff/rotation/logs/snapshot/${DT}.log"
echo "log_file="${log_file} | tee -a ${log_file}
echo "DT="${DT} | tee -a ${log_file}
echo "START_DT_HOUR"=${START_DT_HOUR} | tee -a ${log_file}
echo "END_DT_HOUR"=${END_DT_HOUR}  | tee -a ${log_file}
echo "ROTATION_CONFIG_FILE="${ROTATION_CONFIG_FILE}  | tee -a ${log_file}
echo "OUTPUT_PATH="${OUTPUT_PATH}  | tee -a ${log_file}
echo "START_TIME="${START_TIME}  | tee -a ${log_file}
echo "END_TIME="${END_TIME}  | tee -a ${log_file}

if [ ! -d $OUTPUT_PATH ]; then
 mkdir $OUTPUT_PATH
fi

DT_HOUR_FORMAT=$(date +%Y-%m-%d_ -d "$START_DT_HOUR")
FILE_OUTPUT_PATH=${OUTPUT_PATH}${DT_HOUR_FORMAT}
echo "OUTPUT_PATH="${OUTPUT_PATH} | tee -a ${log_file}
echo "FILE_OUTPUT_PATH="${FILE_OUTPUT_PATH} | tee -a ${log_file}

echo `date`" =============== Job Start ===========" | tee -a ${log_file}


echo `date`" =============== dump rotation files from couchbase by the date $DT===========" | tee -a ${log_file}
java -cp ${bin}/../lib/couchbase-tool-*.jar com.ebay.traffic.chocolate.couchbase.DumpRotationToRno ${ROTATION_CONFIG_FILE} ${START_TIME} ${END_TIME} ${FILE_OUTPUT_PATH}

rc=$?
if [[ ${rc} != 0 ]]; then
   echo `date`"=====================================================dumpFromCouchbase ERROR!!======================================================" | tee -a ${log_file}
   exit ${rc}
else
   echo "=============== dump  data from couchbase done  ===========" | tee -a ${log_file}
   echo `date`"=====================================================dumpFromCouchbase is completed======================================================" | tee -a ${log_file}
fi

HDP=/user/b_marketing_tracking/rotation/rno_daily
HDP_UPDATE_ROTATIONS=/user/b_marketing_tracking/rotation/rno_daily/new_update/rotations
HDP_UPDATE_CAMPAIGNS=/user/b_marketing_tracking/rotation/rno_daily/new_update/campaigns

echo `date`"=====================================================Apollo_rno -- LoadData started======================================================" | tee -a ${log_file}
echo `date`"------ Apollo_rno -- LoadData started~~~" | tee -a ${log_file}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -rm -r ${HDP_UPDATE_ROTATIONS}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -mkdir ${HDP_UPDATE_ROTATIONS}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -copyFromLocal ${OUTPUT_PATH}*'_rotations.txt' ${HDP_UPDATE_ROTATIONS}

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -rm -r ${HDP_UPDATE_CAMPAIGNS}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -mkdir ${HDP_UPDATE_CAMPAIGNS}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -copyFromLocal ${OUTPUT_PATH}*'_campaigns.txt' ${HDP_UPDATE_CAMPAIGNS}

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -rm -r ${HDP}/dt=${DT}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -put ${OUTPUT_PATH} ${HDP}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -ls ${HDP}/dt=${DT} | tee -a ${log_file}
echo `date`"=====================================================Apollo_rno -- LoadData Ended======================================================" | tee -a ${log_file}

HDP_HERCULES=hdfs://hercules/apps/b_marketing_tracking/rotation/hercules_daily
HDP_UPDATE_ROTATIONS_HERCULES=hdfs://hercules/sys/edw/imk/im_tracking/rotation/dw_mpx_rotations_ups/snapshot
HDP_UPDATE_CAMPAIGNS_HERCULES=hdfs://hercules/sys/edw/imk/im_tracking/rotation/dw_mpx_campaigns_ups/snapshot

echo `date`"=====================================================Hercules -- LoadData started======================================================" | tee -a ${log_file}
echo `date`"------ Hercules -- LoadData started~~~" | tee -a ${log_file}
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -rm ${HDP_UPDATE_ROTATIONS_HERCULES}/*
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -copyFromLocal ${OUTPUT_PATH}*'_rotations.txt' ${HDP_UPDATE_ROTATIONS_HERCULES}

/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -rm ${HDP_UPDATE_CAMPAIGNS_HERCULES}/*
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -copyFromLocal ${OUTPUT_PATH}*'_campaigns.txt' ${HDP_UPDATE_CAMPAIGNS_HERCULES}

/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -rm -r ${HDP_HERCULES}/dt=${DT}
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -put ${OUTPUT_PATH} ${HDP_HERCULES}
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls ${HDP_HERCULES}/dt=${DT} | tee -a ${log_file}
echo `date`"=====================================================Hercules -- LoadData Ended======================================================" | tee -a ${log_file}