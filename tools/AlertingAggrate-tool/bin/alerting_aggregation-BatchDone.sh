#!/usr/bin/env bash
CLUSTER=$1
DIFF=1
DATE=`date --date=$DIFF" days ago" +%Y.%m.%d`
echo "DATE:${DATE}"
TMP_DIR="/datashare/mkttracking/tools/AlertingAggrate-tool-imk-v3/temp/"

echo "Start getting hourly done file for apollo-rno cluster."
DATE1=`date +%Y%m%d`
DATE2=`date --date=$DIFF" days ago" +%Y%m%d`
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/watch/$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "apollo-rno" > ${TMP_DIR}/all_done_files/all_done_files.txt
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/watch/$DATE2 | grep -v "^$" | awk '{print $NF}' | grep "apollo-rno" >> ${TMP_DIR}/all_done_files/all_done_files.txt
echo "Finish getting hourly done file for apollo-rno cluster."


echo "Start getting hourly done file for hercules-lvs cluster."
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/apps/b_marketing_tracking/watch/$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" >> ${TMP_DIR}/all_done_files/all_done_files.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/apps/b_marketing_tracking/watch/$DATE2 | grep -v "^$" | awk '{print $NF}' | grep "hercules" >> ${TMP_DIR}/all_done_files/all_done_files.txt
echo "Finish getting hourly done file for hercules-lvs cluster."


echo "Start AAMainBatchDone application."
cd /datashare/mkttracking/tools/AlertingAggrate-tool-imk-v3/lib
java -cp ./AlertingAggrate-tool-3.7.1-RELEASE-fat.jar com.ebay.traffic.chocolate.BatchDoneJob $DATE mx.vip.ebay.com Marketing-Tracking-oncall@ebay.com,DL-eBay-Chocolate-GC@ebay.com batchDone
echo "AAMainBatchDone application end."

