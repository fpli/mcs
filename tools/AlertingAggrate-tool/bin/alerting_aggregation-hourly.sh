#!/usr/bin/env bash

DIFF=1
DATE=`date --date=$DIFF" days ago" +%Y.%m.%d`

echo "Start getting hourly done file for hercules-lvs cluster."
DATE1=`date +%Y%m%d`
DATE2=`date --date=$DIFF" days ago" +%Y%m%d`
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/apps/b_marketing_tracking/watch/$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/hercules_done_files.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/apps/b_marketing_tracking/watch/$DATE2 | grep -v "^$" | awk '{print $NF}' | grep "hercules" >> /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/hercules_done_files.txt
echo "Finish getting hourly done file for hercules-lvs cluster."

echo "Start getting epn report data."
DATE3=`date --date= +%Y-%m-%d`
rm /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count/*.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs dfs -get hdfs://hercules/apps/b_marketing_tracking/alert/epn/$DATE3/hourlyClickCount/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count
mv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count/hourlyClickCount.csv
echo "end getting epn report data."

echo "Start getting epn hourly file from apollo-rno and hercules-lvs."
DATE4=`date --date= +%Y-%m-%d`
DATE5=`date --date="1 days ago" +%Y-%m-%d`
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -ls viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt/click/click_dt=$DATE4 | grep -v "^$" | awk '{print $7, $8}' > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_monitor_epn/today/file-apollorno-list.csv
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -ls viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt/click/click_dt=$DATE5 | grep -v "^$" | awk '{print $7, $8}' > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_monitor_epn/yesterday/file-apollorno-list.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/sys/edw/imk/im_tracking/epn/ams_click/snapshot/click_dt=$DATE4 | grep -v "^$" | awk '{print $7, $8}' > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_monitor_epn/today/file-herculeslvs-list.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/sys/edw/imk/im_tracking/epn/ams_click/snapshot/click_dt=$DATE5 | grep -v "^$" | awk '{print $7, $8}' > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_monitor_epn/yesterday/file-herculeslvs-list.csv
DATE6=`date --date= +%Y%m%d`
DATE7=`date --date="1 days ago" +%Y%m%d`
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -ls viewfs://apollo-rno/apps/b_marketing_tracking/watch/$DATE6 | grep -v "^$" | awk '{print $8}' | grep "ams_click_hourly" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_monitor_epn/today/donefile-apollorno-list.csv
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -ls viewfs://apollo-rno/apps/b_marketing_tracking/watch/$DATE7 | grep -v "^$" | awk '{print $8}' | grep "ams_click_hourly" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_monitor_epn/yesterday/donefile-apollorno-list.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/apps/b_marketing_tracking/watch/$DATE6 | grep -v "^$" | awk '{print $8}' | grep "ams_click_hourly" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_monitor_epn/today/donefile-herculeslvs-list.csv
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/apps/b_marketing_tracking/watch/$DATE7 | grep -v "^$" | awk '{print $8}' | grep "ams_click_hourly" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_monitor_epn/yesterday/donefile-herculeslvs-list.csv
echo "end getting epn hourly file from apollo-rno and hercules-lvs."

echo "Start getting imk hourly count."
filePath=/datashare/mkttracking/tools/AlertingAggrate-tool/temp/imk_hourly_count/
rm -r /datashare/mkttracking/tools/AlertingAggrate-tool/temp/imk_hourly_count/*
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -get viewfs://apollo-rno/apps/b_marketing_tracking/alert/imk/temp/hourlyClickCount/* /datashare/mkttracking/tools/AlertingAggrate-tool/temp/imk_hourly_count
cat ${filePath}channel_name=ROI/*.csv > ${filePath}channel_name=ROI/hourlyClick.csv
cat ${filePath}channel_name=PaidSearch/*.csv > ${filePath}channel_name=PaidSearch/hourlyClick.csv
cat ${filePath}channel_name=NaturalSearch/*.csv > ${filePath}channel_name=NaturalSearch/hourlyClick.csv
cat ${filePath}channel_name=Display/*.csv > ${filePath}channel_name=Display/hourlyClick.csv
cat ${filePath}channel_name=SocialMedia/*.csv > ${filePath}channel_name=SocialMedia/hourlyClick.csv
echo "Finish getting imk hourly count."

echo "Start AAMain application."
java -cp AlertingAggrate-tool-3.4.2-RELEASE-fat.jar com.ebay.traffic.chocolate.AAMain $DATE 10.89.168.20 lli5@ebay.com,lxiong1@ebay.com,huiclu@ebay.com,zhofan@ebay.com,yliu29@ebay.com,shuangxu@ebay.com,jialili1@ebay.com,xiangli4@ebay.com,fechen@ebay.com,zhiyuawang@ebay.com,zjian@ebay.com hourly
echo "AAMain application end."