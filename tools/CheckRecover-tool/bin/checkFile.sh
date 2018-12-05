#!/usr/bin/env bash

NANOTIMESTAMP=`date +%s%N`
TIMESTAMP=$(($NANOTIMESTAMP/1000000))
ESURL=http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200

echo $TIMESTAMP
echo $ESURL

/apache/spark/bin/spark-submit \
    --class com.ebay.traffic.chocolate.job.CheckJob \
    --master yarn \
    --deploy-mode client \
    CheckRecover-tool-*.jar "CheckJob" "yarn" "" $IMESTAMP "task.xml" "http://10.148.181.34:9200"
