#!/bin/bash

export HADOOP_USER_NAME=hdfs

# keep the data for 7 days
DIFF=8
DATE=`date --date=$DIFF" days ago" +%Y-%m-%d`
echo $DATE

hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/EPN/capping/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/EPN/dedupe/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/DISPLAY/capping/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/DISPLAY/dedupe/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/DISPLAY/imkDump/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/PAID_SEARCH/capping/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/PAID_SEARCH/dedupe/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/PAID_SEARCH/imkDump/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/ROI/capping/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/ROI/dedupe/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/ROI/imkDump/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/SOCIAL_MEDIA/capping/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/SOCIAL_MEDIA/dedupe/date=$DATE
hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/SOCIAL_MEDIA/imkDump/date=$DATE

hdfs dfs -rm -r hdfs://slickha/apps/tracking-events/crabDedupe/date=$DATE

# keep the data for 60 days
DIFF60=60
DATE60=`date --date=$DIFF60" days ago" +%Y-%m-%d`
echo $DATE60

hdfs dfs -rm -r hdfs://elvisha/apps/epn-nrt/click/date=$DATE60
hdfs dfs -rm -r hdfs://elvisha/apps/epn-nrt/impression/date=$DATE60