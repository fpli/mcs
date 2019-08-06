#!/usr/bin/env bash

DIFF=1
DATE=`date --date=$DIFF" days ago" +%Y.%m.%d`

java -cp /datashare/mkttracking/tools/AlertingAggrate-tool/lib/AlertingAggrate-tool-3.4.2-RELEASE-fat.jar com.ebay.traffic.chocolate.AAMain $DATE chocolateclusteres-app-private-11.stratus.lvs.ebay.com 10.89.168.20 /datashare/mkttracking/tools/AlertingAggrate-tool/conf/metric.xml lxiong1@ebay.com,huiclu@ebay.com,zhofan@ebay.com,yliu29@ebay.com,shuangxu@ebay.com,jialili1@ebay.com,xiangli4@ebay.com,fechen@ebay.com,zhiyuawang@ebay.com,zjian@ebay.com