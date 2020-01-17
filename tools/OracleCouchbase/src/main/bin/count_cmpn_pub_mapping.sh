#!/usr/bin/env bash

usage="Usage: count_cmpn_pub_mapping.sh"

startTime=$(date --date="1 days ago" +%Y-%m-%d)" 00:00:00"
endTime=$(date +%Y-%m-%d)" 00:00:00"

echo "startTime=$startTime"
echo "endTime=$endTime"

java -Dspring.profiles.active=Production -jar /datashare/mkttracking/tools/AlertingAggrate-tool/lib/OracleCouchbase-0.8.3-RELEASE.jar "$startTime" "$endTime" prod /datashare/mkttracking/tools/AlertingAggrate-tool/temp/oracle/epn_campaign_publisher_mapping.csv