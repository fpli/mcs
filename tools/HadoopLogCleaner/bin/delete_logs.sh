#!/usr/bin/env bash

dt=`date +%Y.%m.%d`

echo "" > /datashare/mkttracking/tools/HadoopLogCleaner/bin/nohup.out

for line in `cat /datashare/mkttracking/tools/HadoopLogCleaner/temp/dir_${dt}.txt`
do
    hdfs dfs -rm -r $line
done