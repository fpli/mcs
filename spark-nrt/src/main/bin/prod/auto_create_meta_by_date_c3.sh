#!/usr/bin/env bash

set -x
# shellcheck disable=SC2034
usage="Usage:auto_create_meta_by_date_c3.sh"1

date=${date -d '2 days ago' +%Y-%m-%d}
echo "date:${date}"

export HADOOP_USER_NAME=chocolate
work_path="hdfs://elvisha/apps/b_marketing_tracking/tracking-events"
local_path="/datashare/mkttracking/test"
# shellcheck disable=SC2164
cd $local_path;
channel_file_list_file="${local_path}/channel_file_list_file.txt"

function createMeta() {
    channel=$1;
    local_channel_path="${local_path}/${date}/${channel}"
    # shellcheck disable=SC2034
    work_channel_path="${work_path}/${channel}/capping/date=${date}"
    dest_channel_path_old_test="hdfs://slickha/apps/b_marketing_tracking/tracking-events-workdir-old-test/meta/EPN/output/capping"
    dest_channel_path_new_test="hdfs://slickha/apps/b_marketing_tracking/tracking-events-workdir-new-test/meta/EPN/output/capping"
    if [ ! -d "${local_channel_path}" ]; then
      mkdir -p "${local_channel_path}"
    fi
    # shellcheck disable=SC2164
    cd "$local_channel_path"
    rm ./capping_output_*.meta.epnnrt
    hdfs dfs -ls "${work_path}/${channel}/capping/date=${date}" | grep -v "^$" | awk '{print $NF}' | grep "part" > $channel_file_list_file
    # shellcheck disable=SC2006
    # shellcheck disable=SC2002
    channel_files=`cat ${channel_file_list_file} | tr "\n" " "`
    # shellcheck disable=SC2006
    # shellcheck disable=SC2002
    # shellcheck disable=SC2126
    file_total_count=`cat ${channel_file_list_file} | grep -v "^$" | wc -l`
    file_count=0
    file_index=0
    meta_file_header="{\"metaFiles\":[{\"date\":\"date=${date}\",\"files\":["
    meta_file_footer="]}]}"
    # shellcheck disable=SC2006
    timestamp=`date +%s`
    for one_file in ${channel_files}
    do
      # shellcheck disable=SC2007
      file_index=$[file_index+1];
      # shellcheck disable=SC2007
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
        echo "$meta_file_detail" > "capping_output_${timestamp}${file_count}.meta.epnnrt"
      fi
    done

    echo "hdfs dfs -rm ${dest_channel_path_old_test}/capping_output_*.meta.epnnrt"
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -rm "${dest_channel_path_old_test}/capping_output_*.meta.epnnrt"
    echo "hdfs dfs -put capping_output_*.meta.epnnrt ${dest_channel_path_old_test}"
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -put capping_output_*.meta.epnnrt ${dest_channel_path_old_test}

    echo "hdfs dfs -rm ${dest_channel_path_new_test}/capping_output_*.meta.epnnrt"
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -rm "${dest_channel_path_new_test}/capping_output_*.meta.epnnrt"
    echo "hdfs dfs -put capping_output_*.meta.epnnrt ${dest_channel_path_new_test}"
    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -put capping_output_*.meta.epnnrt ${dest_channel_path_new_test}
}

createMeta EPN
