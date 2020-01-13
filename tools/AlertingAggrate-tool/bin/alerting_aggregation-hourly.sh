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
hdfs dfs -get hdfs://elvisha/apps/alert/epn/$DATE3/hourlyClickCount/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count
mv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count/hourlyClickCount.csv
echo "end getting epn report data."

echo "Start AAMain application."
java -cp AlertingAggrate-tool-3.4.2-RELEASE-fat.jar com.ebay.traffic.chocolate.AAMain $DATE 10.89.168.20 lli5@ebay.com,lxiong1@ebay.com,huiclu@ebay.com,zhofan@ebay.com,yliu29@ebay.com,shuangxu@ebay.com,jialili1@ebay.com,xiangli4@ebay.com,fechen@ebay.com,zhiyuawang@ebay.com,zjian@ebay.com hourly
echo "AAMain application end."