#!/usr/bin/env bash

set -x
usage="Usage:auto_create_meta_by_date_c3.sh"

date=`date -d '5 days ago' +%Y-%m-%d`

export HADOOP_USER_NAME=chocolate
work_path="hdfs://elvisha/apps/tracking-events"
local_path="/datashare/mkttracking/test"
cd $local_path;
channel_file_list_file="${local_path}/channel_file_list_file.txt"

function createMeta() {
    channel=$1;
    local_channel_path="${local_path}/${date}/${channel}"
    dest_channel_path_old_test="hdfs://slickha/apps/tracking-events-workdir-old-test/meta/EPN/output/capping"
    dest_channel_path_new_test="hdfs://slickha/apps/tracking-events-workdir-new-test/meta/EPN/output/capping"
    if [ ! -d "${local_channel_path}" ]; then
      mkdir -p "${local_channel_path}"
    fi
    cd "$local_channel_path"
    rm ./capping_output_*.meta.epnnrt_v2
    hdfs dfs -ls "${work_path}/${channel}/capping/date=${date}" | grep -v "^$" | awk '{print $NF}' | grep "part" > $channel_file_list_file
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
        echo "$meta_file_detail" > "capping_output_${timestamp}${file_count}.meta.epnnrt_v2"
      fi
    done

    echo "hdfs dfs -rm ${dest_channel_path_old_test}/capping_output_*.meta.epnnrt_v2"
    hdfs dfs -rm "${dest_channel_path_old_test}/capping_output_*.meta.epnnrt_v2"
    echo "hdfs dfs -put capping_output_*.meta.epnnrt_v2 ${dest_channel_path_old_test}"
    hdfs dfs -put capping_output_*.meta.epnnrt_v2 ${dest_channel_path_old_test}

    echo "hdfs dfs -rm ${dest_channel_path_new_test}/capping_output_*.meta.epnnrt_v2"
    hdfs dfs -rm "${dest_channel_path_new_test}/capping_output_*.meta.epnnrt_v2"
    echo "hdfs dfs -put capping_output_*.meta.epnnrt_v2 ${dest_channel_path_new_test}"
    hdfs dfs -put capping_output_*.meta.epnnrt_v2 ${dest_channel_path_new_test}
}

createMeta EPN
