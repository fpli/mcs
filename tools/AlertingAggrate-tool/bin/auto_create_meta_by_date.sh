#!/usr/bin/env bash

set -x
usage="Usage:auto_create_meta_by_date.sh [date]"

if [ $# -lt 1 ]; then
  exit 1
fi

date=$1;
echo "date:${date}"

export HADOOP_USER_NAME=chocolate
work_path="hdfs://elvisha/apps/tracking-events"
local_path="/datashare/mkttracking/test"
cd $local_path;
channel_file_list_file="${local_path}/channel_file_list_file.txt"

function createMeta() {
    channel=$1;
    local_channel_path="${local_path}/${date}/${channel}"
    work_channel_path="${work_path}/${channel}/dedupe/date=${date}"
    dest_channel_path="hdfs://elvisha/apps/tracking-events-workdir-test-imk/meta/${channel}/output/dedupe"
    if [ ! -d "${local_channel_path}" ]; then
      mkdir -p ${local_channel_path}
    fi
    cd $local_channel_path
    rm ./dedupe_output_*.meta.imketl
    hdfs dfs -ls "hdfs://elvisha/apps/tracking-events/${channel}/dedupe/date=${date}" | grep -v "^$" | awk '{print $NF}' | grep "part" > $channel_file_list_file
    channel_files=`cat ${channel_file_list_file} | tr "\n" " "`
    file_total_count=`cat ${channel_file_list_file} | grep -v "^$" | wc -l`
    file_count=0
    file_index=0
    meta_file_header="{\"metaFiles\":[{\"date\":\"date=${date}\",\"files\":["
    meta_file_footer="]}]}"
    timestamp=`date +%s`
    for one_file in ${channel_files}
    do
      file_index=$[file_index+1];
      file_count=$[file_count+1];
      if [ $file_index -eq 1 ]; then
          meta_file_detail=$meta_file_header;
      fi
      meta_file_detail="${meta_file_detail}\"${one_file}\""
      if [[ $file_index -lt 5 ]] && [[ $file_count -lt $file_total_count ]]; then
          meta_file_detail="${meta_file_detail},"
      else
        meta_file_detail="${meta_file_detail}${meta_file_footer}"
        file_index=0
        echo $meta_file_detail > "dedupe_output_${timestamp}${file_count}.meta.imketl"
      fi
    done

    echo "hdfs dfs -rm ${dest_channel_path}/dedupe_output_*.meta.imketl"
    #hdfs dfs -rm "${dest_channel_path}/dedupe_output_*.meta.imketl"
    echo "hdfs dfs -put dedupe_output_*.meta.imketl ${dest_channel_path}"
    #hdfs dfs -put dedupe_output_*.meta ${dest_channel_path}
}

createMeta "DISPLAY"
createMeta "PAID_SEARCH"
createMeta "ROI"
createMeta "SEARCH_ENGINE_FREE_LISTINGS"
createMeta "SOCIAL_MEDIA"
