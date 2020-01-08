#!/usr/bin/env bash
export HADOOP_USER_NAME=hdfs

DATE=`date --date= +%Y-%m-%d`

rm /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourlyClickCount/*.csv
hdfs dfs -get hdfs://elvisha/apps/alert/epn/$DATE/hourlyClickCount/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count
mv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count/hourlyClickCount.csv


java -cp /datashare/mkttracking/tools/AlertingAggrate-tool/lib/AlertingAggrate-tool-3.4.2-RELEASE-fat.jar com.ebay.traffic.chocolate.EPNAlertMain 10.89.168.20 /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count/hourlyClickCount.csv lli5@ebay.com,lxiong1@ebay.com,huiclu@ebay.com,zhofan@ebay.com,yliu29@ebay.com,shuangxu@ebay.com,jialili1@ebay.com,xiangli4@ebay.com,fechen@ebay.com,zhiyuawang@ebay.com,zjian@ebay.com hourly