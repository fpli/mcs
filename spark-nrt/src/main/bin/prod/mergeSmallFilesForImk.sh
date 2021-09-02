#!/bin/bash
set -x

TABLE="im_tracking.imk_rvr_trckng_event_v2"
PARTITION_NAME="dt"
PARTITION=$(date -d -7days +%Y-%m-%d)
PARTITION_NUM=240
OUTPUT_DIR="hdfs://hercules/apps/b_marketing_tracking/merge/imk_rvr_trckng_event_v2"
BACKUP_DIA="hdfs://hercules/apps/b_marketing_tracking/merge_backup/imk_rvr_trckng_event_v2"
SOURCE_DIR="hdfs://hercules/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_v2/snapshot"
COMMAND_HDFS="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs "

#check SOURCE_DIR if exist
DATA_DIR=SOURCE_DIR+"/"+PARTITION_NAME + "=" + PARTITION
$COMMAND_HDFS -test -e $DATA_DIR
if [ $? -ne 0 ]; then
  echo "data source not exist: $SOURCE_DIR"
  exit 1
fi
# mergeSmallFiles
/datashare/mkttracking/jobs/merge-small-files/bin/mergeSmallFiles.sh hercules $TABLE $PARTITION_NAME $PARTITION $PARTITION_NUM $OUTPUT_DIR
# check merge status
MERGE_STATUS=$?
MERGE_DIR="$OUTPUT_DIR/$PARTITION_NAME=$PARTITION"
MERGE_FILE_COUNT=$($COMMAND_HDFS dfs -ls $MERGE_DIR | grep -v "^$" | awk '{print $NF}' | grep "parquet" | wc -l)
if [ $MERGE_STATUS -ne 0 -o $MERGE_FILE_COUNT -ne $PARTITION_NUM ]; then
  echo "Failed merge"
  echo -e "Failed to merge $TABLE file, $PARTITION_NAME=$PARTITION!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[MERGE FILE ERROR] Failed to merge $TABLE file!" -v DL-eBay-Chocolate-GC@ebay.com,Marketing-Tracking-oncall@ebay.com
  exit 1
fi
# replace data
$COMMAND_HDFS dfs -mv $DATA_DIR $BACKUP_DIA && $COMMAND_HDFS dfs -mv $MERGE_DIR $DATA_DIR
if [ $? -ne 0 ]; then
  echo "Failed replace"
  echo -e "Failed to merge $TABLE file, $PARTITION_NAME=$PARTITION!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[MERGE FILE ERROR] Failed to merge $TABLE file!" -v DL-eBay-Chocolate-GC@ebay.com,Marketing-Tracking-oncall@ebay.com
  exit 1
fi

$COMMAND_HDFS dfs -rm -r $BACKUP_DIA
echo -e "Success to merge $TABLE file, $PARTITION_NAME=$PARTITION!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[MERGE FILE SUCCESS] Success to merge $TABLE file!" -v DL-eBay-Chocolate-GC@ebay.com,Marketing-Tracking-oncall@ebay.com
