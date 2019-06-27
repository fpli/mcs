#!/usr/bin/env bash
####################################################################################################################
# epnnrt done file generation script. Unless we meet all the criteria below, epnnrt is not done
# 1. Today's data are being generated
# 2. Message timestamp are newer than now-1h
# 3. Check yesterday's data were handled
# 4. Check yesterday's data are fully sent
####################################################################################################################
log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt/done_${log_dt}.log"

######################################### Check the file of today has been generate ################################
DT_TODAY=$(date +%Y-%m-%d)
LOCAL_PATH=/datashare/mkttracking/data/epn-nrt/date=${DT_TODAY}   #Local file path which contains the epnnrt click result files
clickFileCnt=`ls ${LOCAL_PATH}/dw_ams.ams_clicks_cs_* |wc -l`
impFileCnt=`ls ${LOCAL_PATH}/dw_ams.ams_imprsn_cntnr_cs_*|wc -l`

if [[ clickFileCnt -le 1 || impFileCnt -le 1 ]]; then
     echo "chocolate-ePN ${DT}'s NRT not generated!!!!" | mail -s "NRT delayed!!!!(Today's Files not generated)" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
     exit 1
fi

######################################### Check message lag from SinkAndDedupe job ################################
LAG_THRESHOLD=3600000
echo "the threshold of kafka message: "${LAG_THRESHOLD} | tee -a ${log_file}
LAST_TS_PATH=/apps/tracking-events-workdir/last_ts/EPN/*
last_ts=`hdfs dfs -cat ${LAST_TS_PATH} | sort -n | head -1`
echo "timestamp of last message: "${last_ts} | tee -a ${log_file}
now_ts=$(($(date +%s%N)/1000000))
echo "timestamp of now: "${now_ts} | tee -a ${log_file}
message_lag=$(($now_ts-$last_ts))
echo "lag of message: "${message_lag} | tee -a ${log_file}
if [[ message_lag -gt ${LAG_THRESHOLD} ]]; then
     echo "chocolate-ePN ${DT}'s NRT not generated because of message lag!!!!" | mail -s "NRT delayed!!!!(Message lag)" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
     exit 1
fi

######################################### Check the file of yesterday has been completed ################################
DT=$(date +%Y-%m-%d -d "`date` - 1 day")
HDP_CLICK=/apps/epn-nrt/click/date=${DT}/                   #chocolate hdfs files
LOCAL_PATH=/datashare/mkttracking/data/epn-nrt/date=${DT}   #Local file path which contains the epnnrt click result files
HDP_IMP=/apps/epn-nrt/impression/date=${DT}/                #Local file path which contains the epnnrt impression result files

echo "HDP_CLICK="${HDP_CLICK} | tee -a ${log_file}
echo "LOCAL_PATH="${LOCAL_PATH} | tee -a ${log_file}
echo "HDP_IMP="${HDP_IMP} | tee -a ${log_file}

if [[ ! -d "${LOCAL_PATH}/" ]]; then
    echo "chocolate-ePN ${DT}'s NRT data is NOT generated~!!!" | mail -s "No NRT result!!!!" DL-eBay-Chocolate-GC@ebay.com  | tee -a ${log_file}
    exit 1;
fi

cd ${LOCAL_PATH}
################ Check Click Files ################################
hdfs dfs -ls -C ${HDP_CLICK} > ${LOCAL_PATH}/all_click_files.txt
ls ${LOCAL_PATH}/dw_ams.ams_clicks_cs_*.processed > ${LOCAL_PATH}/all_click_processed.txt

HDP_CLICK_FORMAT=`echo "${HDP_CLICK}" | sed 's:\/:\\\/:g'`
sed -i -E "s/$HDP_CLICK_FORMAT//g" ${LOCAL_PATH}/all_click_files.txt
LOCAL_PATH_FORMAT=`echo "${LOCAL_PATH}/" | sed 's:\/:\\\/:g'`
sed -i -E "s/.processed//g" ${LOCAL_PATH}/all_click_processed.txt
sed -i -E "s/$LOCAL_PATH_FORMAT//g" ${LOCAL_PATH}/all_click_processed.txt

sort all_click_files.txt > sorted_all_click_files.txt
sort all_click_processed.txt > sorted_all_click_processed.txt

diff_click_line=`diff sorted_all_click_files.txt sorted_all_click_processed.txt |wc -l`
if [[ ${diff_click_line} -lt 1 ]]; then
    echo "chocolate-ePN ${DT}'s NRT completed(Click)." | mail -s "NRT completed(Click)" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
else
    echo "chocolate-ePN ${DT}'s NRT delayed(Click)!!!!" | mail -s "NRT delayed!!!!(Click)" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit 1
fi

################ Check Impression Files ################################
hdfs dfs -ls -C ${HDP_IMP} > ${LOCAL_PATH}/all_imp_files.txt
ls ${LOCAL_PATH}/dw_ams.ams_imprsn_cntnr_cs_*.processed > ${LOCAL_PATH}/all_imp_processed.txt

HDP_IMP_FORMAT=`echo "${HDP_IMP}" | sed 's:\/:\\\/:g'`
sed -i -E "s/$HDP_IMP_FORMAT//g" all_imp_files.txt
LOCAL_PATH_FORMAT=`echo "${LOCAL_PATH}/" | sed 's:\/:\\\/:g'`
sed -i -E "s/.processed//g" all_imp_processed.txt
sed -i -E "s/$LOCAL_PATH_FORMAT//g" all_imp_processed.txt

sort all_imp_files.txt > sorted_all_imp_files.txt
sort all_imp_processed.txt > sorted_all_imp_processed.txt

diff_imp_line=`diff sorted_all_imp_files.txt sorted_all_imp_processed.txt |wc -l`
if [[ ${diff_imp_line} -lt 1 ]]; then
    echo "chocolate-ePN ${DT}'s NRT completed(Impression)." | mail -s "NRT completed(Impression)" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
else
    echo "chocolate-ePN ${DT}'s NRT delayed(Impression)!!!!" | mail -s "NRT delayed!!!!(Impression)" DL-eBay-Chocolate-GC@ebay.com | tee -a ${log_file}
    exit 1
fi

################ Generate Done file and send it to ETL ################################
DONE_FILE="epn_$(date +%Y%m%d -d "`date` - 1 day").done"
touch "$DONE_FILE"

/datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/sendToETL.sh ${DONE_FILE} ${log_file}
