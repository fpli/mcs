#!/usr/bin/env bash
####################################################################################
# Send epn nrt result file to ETL and Apollo-RNO. Runs every 5 mins.
# Here is the contract: if it's not 1AM today yet, send yesterday's data.
# Otherwise, consider yesterday's job is done. Send today's data.
####################################################################################
HOUR=$(date +%H)
if [[ ${HOUR} -lt 1 ]]; then
  DT=$(date +%Y-%m-%d -d "`date` - 1 hour")
else
  DT=$(date +%Y-%m-%d)
fi

log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt/send_${log_dt}.log"

##################### Send Job Parameters ##################
HDP_CLICK=/apps/epn-nrt/click/date=${DT}/                  #chocolate hdfs files
LOCAL_PATH=/datashare/mkttracking/data/epn-nrt/date=${DT}   #Local file path which contains the epnnrt click result files
HDP_IMP=/apps/epn-nrt/impression/date=${DT}/                #Local file path which contains the epnnrt impression result files
FILE_LIST=${LOCAL_PATH}/files.txt                           #file list which contains all file names in local machine path
HDP=/apps/b_marketing_tracking/chocolate/epnnrt             #the click file path on apollo_rno


if [[ ! -d "${LOCAL_PATH}" ]]; then
  mkdir -p ${LOCAL_PATH}
  chmod -R 777 ${LOCAL_PATH}
fi

echo "HDP_CLICK="${HDP_CLICK} | tee -a ${log_file}
echo "LOCAL_PATH="${LOCAL_PATH} | tee -a ${log_file}
echo "HDP_IMP="${HDP_IMP} | tee -a ${log_file}
echo "FILE_LIST="${FILE_LIST} | tee -a ${log_file}

export HADOOP_USER_NAME=chocolate

##################### Send Clicks ##################
# Every time the job runs, dump latest file list from hadoop to FILE_LIST.
# Check local dir file names with processed as extension. They are treated as files already sent.
# Hadoop get those files local dir does not contain. Send them and rename them to be *.processed.
# .processed files are later used in epnnrt-done.sh to check if all the files from hadoop are sent.

hdfs dfs -ls -C ${HDP_CLICK} > ${FILE_LIST}

idx=${#HDP_CLICK}
while read p; do
  fileName=${p:$idx}

  cd ${LOCAL_PATH}
  if [ ! -f "${fileName}.processed" ]; then
	  hadoop fs -get ${HDP_CLICK}${fileName}

	  ##################### Send To ETL Sever ##################
	  /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/sendToETL.sh ${fileName} ${log_file}
      ret_clk_etl=$?

    ##################### Send To Apollo_RNO ##################
	  /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/sendToApolloRno.sh  ${HDP}/click/date=${DT}  ${LOCAL_PATH}/${fileName} ${log_file}

	  ret_clk_rno=$?
	  if [[ $ret_clk_etl == 0 && $ret_clk_rno == 0 ]]; then
	    mv ${LOCAL_PATH}/${fileName} ${LOCAL_PATH}/${fileName}.processed
	  fi
  fi

done < ${FILE_LIST}

##################### Send Impressions ##################
hdfs dfs -ls -C ${HDP_IMP} > ${FILE_LIST}

idx=${#HDP_IMP}
while read p; do
  fileName=${p:$idx}

  cd ${LOCAL_PATH}
  if [[ ! -f "${fileName}.processed" ]]; then
	  hadoop fs -get ${HDP_IMP}${fileName}

	  ##################### Send To ETL Sever ##################
	  /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/sendToETL.sh ${fileName} ${log_file}

	  ret_imp_etl=$?
	  ##################### Send To Apollo_RNO ##################
	  /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/sendToApolloRno.sh  ${HDP}/imp/date=${DT}  ${LOCAL_PATH}/${fileName} ${log_file}

	  ret_imp_rno=$?
	  if [[ $ret_imp_etl == 0 && $ret_imp_rno == 0 ]]; then
	    mv ${LOCAL_PATH}/${fileName} ${LOCAL_PATH}/${fileName}.processed
	  fi
  fi
done < ${FILE_LIST}


