#!/bin/bash
set -x

TABLE="im_tracking.imk_rvr_trckng_event_v2"
PARTITION_NAME="dt"
PARTITION=$(date -d -7days +%Y-%m-%d)
PARTITION_NUM=240
OUTPUT_DIR="hdfs://hercules/apps/b_marketing_tracking/merge_file/imk_rvr_trckng_event_v2"
BACKUP_DIA="hdfs://hercules/apps/b_marketing_tracking/merge_file/imk_rvr_trckng_event_v2/backup"
SOURCE_DIR="hdfs://hercules/sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_v2/snapshot"
COMMAND_HDFS="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs "

#check SOURCE_DIR if exist
DATA_DIR="$SOURCE_DIR/$PARTITION_NAME=$PARTITION"
$COMMAND_HDFS dfs -test -e $DATA_DIR
if [ $? -ne 0 ]; then
  echo "data source not exist: $SOURCE_DIR"
  exit 1
fi
# mergeSmallFiles
/datashare/mkttracking/jobs/merge-small-files/bin/mergeSmallFiles.sh hercules $TABLE $PARTITION_NAME $PARTITION $PARTITION_NUM $OUTPUT_DIR
# check merge status
MERGE_STATUS=$?
MERGE_DATA="$OUTPUT_DIR/data/$PARTITION_NAME=$PARTITION"
MERGE_FILE_COUNT=$($COMMAND_HDFS dfs -ls $MERGE_DATA | grep -v "^$" | awk '{print $NF}' | grep "parquet" | wc -l)
if [ $MERGE_STATUS -ne 0 -o $MERGE_FILE_COUNT -ne $PARTITION_NUM ]; then
  echo "Failed merge"
  echo -e "Failed to merge $TABLE file, $PARTITION_NAME=$PARTITION!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[MERGE FILE ERROR] Failed to merge $TABLE file!" -v DL-eBay-Chocolate-GC@ebay.com,Marketing-Tracking-oncall@ebay.com
  exit 1
fi

#Check records number
RESULT=$($COMMAND_HDFS dfs -cat "$OUTPUT_DIR/result/$PARTITION")
echo "RESULT: $RESULT"
RESULTS=(${RESULT//\#/ })
echo "Before merge: ${RESULTS[0]}"
echo "After merge: ${RESULTS[1]}"
if [ ${RESULTS[0]} -ne ${RESULTS[1]} ]; then
  echo "Failed merge, records check failed"
  echo -e "Failed to merge $TABLE file, $PARTITION_NAME=$PARTITION, records check failed!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[MERGE FILE ERROR] Failed to merge $TABLE file!" -v DL-eBay-Chocolate-GC@ebay.com,Marketing-Tracking-oncall@ebay.com
  exit 1
fi
# replace data
TIMESTAMP=$(date +%s)
BACKUP_DATA="$BACKUP_DIA/$PARTITION_NAME=$PARTITION-$TIMESTAMP"
$COMMAND_HDFS dfs -mv $DATA_DIR $BACKUP_DATA && $COMMAND_HDFS dfs -mv $MERGE_DATA $DATA_DIR
if [ $? -ne 0 ]; then
  echo "Failed replace"
  echo -e "Failed to merge $TABLE file, $PARTITION_NAME=$PARTITION!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[MERGE FILE ERROR] Failed to merge $TABLE file!" -v DL-eBay-Chocolate-GC@ebay.com,Marketing-Tracking-oncall@ebay.com
  exit 1
fi

$COMMAND_HDFS dfs -rm -r $BACKUP_DATA
echo -e "Success to merge $TABLE file, $PARTITION_NAME=$PARTITION!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[MERGE FILE SUCCESS] Success to merge $TABLE file!" -v DL-eBay-Chocolate-GC@ebay.com,Marketing-Tracking-oncall@ebay.com
