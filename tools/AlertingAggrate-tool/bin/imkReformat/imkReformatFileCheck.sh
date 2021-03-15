set -x
# count /apps/b_marketing_tracking/delta_test/output_imk files num
# FILE_NAME like 202001
# beginDate like 20200101
# endDate like 20200131
usage="Usage: imkReformatFileCheck.sh [fileName] [beginDate] [endDate]"

if [ $# -lt 3 ]; then
  echo $usage
  exit 1
fi

FILE_NAME=${1}
BEGIN_DATE=${2}
END_DATE=${3}

echo "BEGIN_DATE:$BEGIN_DATE";
echo "END_DATE:$END_DATE";
echo "FILE_NAME:$FILE_NAME";

if [[ -z $BEGIN_DATE ||  -z $END_DATE || $END_DATE < $BEGIN_DATE ]]
then
    echo $usage;
    exit 1;
fi

current_date=$BEGIN_DATE;
echo "current_date:$current_date";

LOCAL_PATH='/datashare/mkttracking/jobs/imkReformate'
COMMAND_HDFS="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs "
IMK_NEW_PATH='/apps/b_marketing_tracking/delta_test/output_imk'
tmp_file="${LOCAL_PATH}/tmp/fileTmp.txt"
file_count="${LOCAL_PATH}/verify/file_count_${FILE_NAME}"
cd ${LOCAL_PATH}
pwd
echo > $file_count;
while [[ $current_date -le $END_DATE ]]; do
    echo "current_date:$current_date";
    echo "begin deal with data in $current_date";
    dt=${current_date:0:4}-${current_date:4:2}-${current_date:6}
    echo "dt:$dt";
    SRC_DIR="$IMK_NEW_PATH/dt=${dt}"
    $COMMAND_HDFS -ls ${SRC_DIR} | grep -v "^$" | awk '{print $NF}' | grep "part" | wc -l > ${tmp_file}
    file_size=`cat ${tmp_file}`;
    echo "${dt}:${file_size}";
    echo "${dt}#${file_size}" >> $file_count;
    current_date=$(date -d"${current_date} 1 days" +"%Y%m%d");
done