#!/usr/bin/env bash

dt=`date +%Y.%m.%d`

for line in `cat /datashare/mkttracking/tools/HadoopLogCleaner/temp/dir_${dt}.txt`
do
    hdfs dfs -rm $line
done