#!/bin/bash

export HADOOP_USER_NAME=hdfs

# keep the data for 8 days
DIFF=8
DATE=`date --date=$DIFF" days ago" +%Y-%m-%d`
echo "DIFF $DIFF"
echo "DATE $DATE"

if [ ! -n "$DATE" ] || [ ! -n "$DIFF" ]; then
  echo "The length of DATE or DIFF is zero, exit."
  exit
else
  echo "Both the length of DATE and DIFF are not zero."
fi

if [ "$DIFF" -lt 8 ]; then
  echo "But diff is less than 8, exit."
  exit
else
  echo "And diff is greater than or equals 8."
fi

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
echo "DIFF60 $DIFF60"
echo "DATE60 $DATE60"

if [ ! -n "$DATE60" ] || [ ! -n "$DIFF60" ]; then
  echo "The length of DATE60 or DIFF60 is zero, exit."
  exit
else
  echo "Both the length of DATE60 and DIFF60 are not zero."
fi

if [ "$DIFF60" -lt 60 ]; then
  echo "But diff is less than 60, exit."
  exit
else
  echo "And diff is greater than or equals 60."
fi

hdfs dfs -rm -r hdfs://elvisha/apps/epn-nrt/click/date=$DATE60
hdfs dfs -rm -r hdfs://elvisha/apps/epn-nrt/impression/date=$DATE60