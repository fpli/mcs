#!/usr/bin/env bash

DIFF=1
DATE=`date --date=$DIFF" days ago" +%Y.%m.%d`

DATE1=`date +%Y%m%d`
DATE2=`date --date=$DIFF" days ago" +%Y%m%d`
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/apps/b_marketing_tracking/watch/$DATE1 | grep -v "^$" | awk '{print $NF}' | grep "hercules" > /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/hercules_done_files.txt
/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -ls hdfs://hercules/apps/b_marketing_tracking/watch/$DATE2 | grep -v "^$" | awk '{print $NF}' | grep "hercules" >> /datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/hercules_done_files.txt

java -cp /datashare/mkttracking/tools/AlertingAggrate-tool/lib/AlertingAggrate-tool-3.4.2-RELEASE-fat.jar com.ebay.traffic.chocolate.AAMain $DATE chocolateclusteres-app-private-11.stratus.lvs.ebay.com 10.89.168.20 /datashare/mkttracking/tools/AlertingAggrate-tool/conf/metric.xml lli5@ebay.com,lxiong1@ebay.com,huiclu@ebay.com,zhofan@ebay.com,yliu29@ebay.com,shuangxu@ebay.com,jialili1@ebay.com,xiangli4@ebay.com,fechen@ebay.com,zhiyuawang@ebay.com,zjian@ebay.com