#!/usr/bin/env bash
export HADOOP_USER_NAME=hdfs

DATE=`date --date= +%Y-%m-%d`

rm /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_click_trend/*.csv
hdfs dfs -get hdfs://elvisha/apps/alert/epn/$DATE/dailyClickTrend/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_click_trend
mv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_click_trend/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_click_trend/dailyClickTrend.csv

rm /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_domain_trend/*.csv
hdfs dfs -get hdfs://elvisha/apps/alert/epn/$DATE/dailyDomainTrend/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_domain_trend
mv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_domain_trend/part-*.csv /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_domain_trend/dailyDomainTrend.csv


java -cp /datashare/mkttracking/tools/AlertingAggrate-tool/lib/AlertingAggrate-tool-3.4.2-RELEASE-fat.jar com.ebay.traffic.chocolate.EPNAlertMain 10.89.168.20 /datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_click_trend/dailyClickTrend.csv,/datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_domain_trend/dailyDomainTrend.csv lli5@ebay.com,lxiong1@ebay.com,huiclu@ebay.com,zhofan@ebay.com,yliu29@ebay.com,shuangxu@ebay.com,jialili1@ebay.com,xiangli4@ebay.com,fechen@ebay.com,zhiyuawang@ebay.com,zjian@ebay.com daily