#!/usr/bin/env bash

dt=`date +%Y.%m.%d`

hdfs dfs -ls hdfs://elvisha/app-logs/chocolate/logs | grep -v "^$" | awk '{print $NF}' | grep "application" > /datashare/mkttracking/tools/HadoopLogCleaner/temp/dir_${dt}.txt