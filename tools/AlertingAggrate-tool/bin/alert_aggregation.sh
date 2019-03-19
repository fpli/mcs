#!/usr/bin/env bash

DIFF=1
DATE=`date --date=$DIFF" days ago" +%Y-%m-%d`

java -cp /mnt/appdl/appdl_nr:t/lib/ com.ebay.traffic.chocolate.AAMain DATE chocolateclusteres-app-private-11.stratus.lvs.ebay.com 10.89.168.20 metric.xml lxiong1@ebay.com