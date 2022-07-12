#!/usr/bin/env bash
export HADOOP_USER_NAME=hdfs

DIFF=1
DATE=`date --date=$DIFF" days ago" +%Y.%m.%d`
echo "DATE:${DATE}"
TMP_DIR="/datashare/mkttracking/tools/AlertingAggrate-tool-imk-v3/temp"

echo "Start getting cluster file list."
DATE1=`date --date=$DIFF" days ago" +%Y-%m-%d`
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -ls hdfs://hercules/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_v2/snapshot/dt=$DATE1/utp* | grep -v "^$" | awk '{print $NF}' | grep "hercules" > ${TMP_DIR}/hercules_files/imk_rv
r_trckng_event_v2.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -ls hdfs://hercules/sys/edw/imk/im_tracking/epn/ams_click_v2/snapshot/click_dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > ${TMP_DIR}/hercules_files/ams_click.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -ls hdfs://hercules/sys/edw/imk/im_tracking/epn/ams_imprsn_v2/snapshot/imprsn_dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > ${TMP_DIR}/hercules_files/ams_impression
.txt

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_v2/dt=$DATE1/utp* | grep -v "^$" | awk '{print $NF}' | grep "apollo" > ${TMP_DIR}/apollo_files/imk_
rvr_trckng_event_v2.txt
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt_v2/click/click_dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "apollo" > ${TMP_DIR}/apollo_files/ams_click.txt
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt_v2/imp/imprsn_dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "apollo" > ${TMP_DIR}/apollo_files/ams_impression.
txt

echo "Start getting epn report data."
DATE2=`date --date= +%Y-%m-%d`
rm ${TMP_DIR}/daily_click_trend/*.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -get hdfs://hercules/apps/b_marketing_tracking/alert/epn/$DATE2/dailyClickTrend_v2/part-*.csv ${TMP_DIR}/daily_click_trend
mv ${TMP_DIR}/daily_click_trend/part-*.csv ${TMP_DIR}/daily_click_trend/dailyClickTrend.csv
rm ${TMP_DIR}/daily_domain_trend/main-*.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -get hdfs://hercules/apps/b_marketing_tracking/alert/epn/$DATE2/dailyDomainTrend_v2/part-*.csv ${TMP_DIR}/daily_domain_trend
mv ${TMP_DIR}/daily_domain_trend/part-*.csv ${TMP_DIR}/daily_domain_trend/dailyDomainTrend.csv
echo "Finish getting epn report data."

echo "Start getting hourly done file for apollo-rno cluster."
DATE1=$(date --date="3 days ago" +%Y%m%d)
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/watch/$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "apollo-rno" > ${TMP_DIR}/apollo_files/apollo_done_files.txt
echo "Finish getting hourly done file for apollo-rno cluster."

echo "Start getting hourly done file for hercules-lvs cluster."
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/apps/b_marketing_tracking/watch/$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > ${TMP_DIR}/hercules_files/hercules_done_files.txt
echo "Finish getting hourly done file for hercules-lvs cluster."

echo "Start AAMain application."
cd /datashare/mkttracking/tools/AlertingAggrate-tool-imk-v3/lib
java -cp ./AlertingAggrate-tool-*.jar com.ebay.traffic.chocolate.AAMain $DATE mx.vip.ebay.com Marketing-Tracking-oncall@ebay.com,DL-eBay-Chocolate-GC@ebay.com daily
echo "AAMain application end."
