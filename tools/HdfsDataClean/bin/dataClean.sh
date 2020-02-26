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

hdfs dfs -ls hdfs://elvisha/apps/tracking-events/*/*/date=$DATE > /datashare/mkttracking/tools/HdfsDataClean/temp/allChannel.txt
hdfs dfs -ls hdfs://slickha/apps/tracking-events/crabDedupe/date=$DATE > /datashare/mkttracking/tools/HdfsDataClean/temp/crabDedupe.txt

hdfs dfs -rm -r hdfs://elvisha/apps/tracking-events/*/*/date=$DATE
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

hdfs dfs -ls hdfs://elvisha/apps/epn-nrt/*/date=$DATE60 > /datashare/mkttracking/tools/HdfsDataClean/temp/epn.txt
hdfs dfs -rm -r hdfs://elvisha/apps/epn-nrt/*/date=$DATE60