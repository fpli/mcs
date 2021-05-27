#!/usr/bin/env bash
export HADOOP_USER_NAME=hdfs

DIFF=1
DATE=`date --date=$DIFF" days ago" +%Y.%m.%d`
echo "DATE:${DATE}"
TMP_DIR="/datashare/mkttracking/tools/AlertingAggrate-tool-imk-v2/temp_v2"

echo "Start getting cluster file list."
DATE1=`date --date=$DIFF" days ago" +%Y-%m-%d`
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -ls hdfs://hercules/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_v2/snapshot/dt=$DATE1/utp* | grep -v "^$" | awk '{print $NF}' | grep "hercules" > ${TMP_DIR}/hercules_files/imk_rvr_trckng_event_v2.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -ls hdfs://hercules/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event/snapshot/dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > ${TMP_DIR}/hercules_files/imk_rvr_trckng_event.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -ls hdfs://hercules/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_dtl/snapshot/dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > ${TMP_DIR}/hercules_files/imk_rvr_trckng_event_dtl.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -ls hdfs://hercules/sys/edw/imk/im_tracking/epn/ams_click_v2/snapshot/click_dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > ${TMP_DIR}/hercules_files/ams_click.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -ls hdfs://hercules/sys/edw/imk/im_tracking/epn/ams_imprsn_v2/snapshot/imprsn_dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > ${TMP_DIR}/hercules_files/ams_impression.txt

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_v2/dt=$DATE1/utp* | grep -v "^$" | awk '{print $NF}' | grep "apollo" > ${TMP_DIR}/apollo_files/imk_rvr_trckng_event_v2.txt
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event/dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "apollo" > ${TMP_DIR}/apollo_files/imk_rvr_trckng_event.txt
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl/dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "apollo" > ${TMP_DIR}/apollo_files/imk_rvr_trckng_event_dtl.txt
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt_v2/click/click_dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "apollo" > ${TMP_DIR}/apollo_files/ams_click.txt
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -ls viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt_v2/imp/imprsn_dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "apollo" > ${TMP_DIR}/apollo_files/ams_impression.txt

hadoop fs -ls hdfs://slickha/apps/epn-nrt-v2/click/date=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "elvisha" > ${TMP_DIR}/chocolate_files/ams_click.txt
hadoop fs -ls hdfs://slickha/apps/epn-nrt-v2/impression/date=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "elvisha" > ${TMP_DIR}/chocolate_files/ams_impression.txt


echo "Finish getting cluster file list."

echo "Start getting td data count."
table_name_arr=(imk_rvr_trckng_event imk_rvr_trckng_event_dtl ams_click ams_imprsn dw_mpx_rotations dw_mpx_campaigns dw_mpx_clients dw_mpx_vendors)
for table_name in ${table_name_arr[@]};
do
    echo ${table_name}
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -cat viewfs://apollo-rno/user/b_marketing_tracking/monitor/td/${table_name}_mozart_merge/part-* > ${TMP_DIR}/td/${table_name}_mozart_merge
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -cat viewfs://apollo-rno/user/b_marketing_tracking/monitor/td/${table_name}_hopper_merge/part-* > ${TMP_DIR}/td/${table_name}_hopper_merge
done
echo "Finish getting td data count."

echo "Start getting epn report data."
DATE2=`date --date= +%Y-%m-%d`
rm ${TMP_DIR}/daily_click_trend/*.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -get hdfs://hercules/apps/b_marketing_tracking/alert/epn/$DATE2/dailyClickTrend_v2/part-*.csv ${TMP_DIR}/daily_click_trend
mv ${TMP_DIR}/daily_click_trend/part-*.csv ${TMP_DIR}/daily_click_trend/dailyClickTrend.csv
rm ${TMP_DIR}/daily_domain_trend/main-*.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -get hdfs://hercules/apps/b_marketing_tracking/alert/epn/$DATE2/dailyDomainTrend_v2/part-*.csv ${TMP_DIR}/daily_domain_trend
mv ${TMP_DIR}/daily_domain_trend/part-*.csv ${TMP_DIR}/daily_domain_trend/dailyDomainTrend.csv
echo "Finish getting epn report data."

echo "Start AAMain application."
java -cp /datashare/mkttracking/tools/AlertingAggrate-tool-imk-v2/lib/AlertingAggrate-tool-*.jar com.ebay.traffic.chocolate.AAMain $DATE mx.vip.ebay.com lli5@ebay.com,zhofan@ebay.com,yliu29@ebay.com,shuangxu@ebay.com,jialili1@ebay.com,xiangli4@ebay.com,fechen@ebay.com,zhiyuawang@ebay.com,zjian@ebay.com,yyang28@ebay.com,yli19@ebay.com,yuhxiao@ebay.com,xuanwwang@ebay.com,Marketing-Tracking-oncall@ebay.com daily
echo "AAMain application end."
