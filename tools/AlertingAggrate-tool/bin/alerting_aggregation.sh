#!/usr/bin/env bash
export HADOOP_USER_NAME=hdfs

DIFF=1
DATE=`date --date=$DIFF" days ago" +%Y.%m.%d`

echo "Start getting hercules-lvs file list."
DATE1=`date --date=$DIFF" days ago" +%Y-%m-%d`
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event/snapshot/dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/imk_rvr_trckng_event.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_dtl/snapshot/dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/imk_rvr_trckng_event_dtl.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/sys/edw/imk/im_tracking/epn/ams_click/snapshot/click_dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/ams_click.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/sys/edw/imk/im_tracking/epn/ams_impression/snapshot/imprsn_dt=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/ams_impression.txt
hadoop fs -ls hdfs://elvisha/apps/epn-nrt/click/date=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "elvisha" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/choco_ams_click.txt
hadoop fs -ls hdfs://elvisha/apps/epn-nrt/impression/date=$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "elvisha" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/choco_ams_impression.txt
echo "Finish getting hercules-lvs file list."

echo "Start getting td data count."
table_name_arr=(imk_rvr_trckng_event imk_rvr_trckng_event_dtl ams_click ams_imprsn dw_mpx_rotations dw_mpx_campaigns dw_mpx_clients dw_mpx_vendors)
for table_name in ${table_name_arr[@]};
do
    echo ${table_name}
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -cat viewfs://apollo-rno/user/b_marketing_tracking/monitor/td/${table_name}_mozart_merge/part-* > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/td/${table_name}_mozart_merge
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -cat viewfs://apollo-rno/user/b_marketing_tracking/monitor/td/${table_name}_hopper_merge/part-* > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/td/${table_name}_hopper_merge
done
echo "Finish getting td data count."

echo "Start getting epn report data."
DATE2=`date --date= +%Y-%m-%d`
rm /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_click_trend/*.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -get hdfs://hercules/apps/b_marketing_tracking/alert/epn/$DATE2/dailyClickTrend/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_click_trend
mv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_click_trend/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_click_trend/dailyClickTrend.csv
rm /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_domain_trend/main-*.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -get hdfs://hercules/apps/b_marketing_tracking/alert/epn/$DATE2/dailyDomainTrend/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_domain_trend
mv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_domain_trend/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_domain_trend/dailyDomainTrend.csv
echo "Finish getting epn report data."

echo "Start AAMain application."
java -cp /datashare/mkttracking/tools/AlertingAggrate-tool/lib/AlertingAggrate-tool-*.jar com.ebay.traffic.chocolate.AAMain $DATE 10.89.168.20 lli5@ebay.com,lxiong1@ebay.com,huiclu@ebay.com,zhofan@ebay.com,yliu29@ebay.com,shuangxu@ebay.com,jialili1@ebay.com,xiangli4@ebay.com,fechen@ebay.com,zhiyuawang@ebay.com,zjian@ebay.com,yyang28@ebay.com,yli19@ebay.com daily
echo "AAMain application end."
