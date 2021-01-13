set -x
# count /apps/b_marketing_tracking/delta_test/output_imk files num
# FILE_NAME like 202001
# beginDate like 20200101
# endDate like 20200131
usage="Usage: imkReformatNewEventTsCheck.sh [fileName] [beginDate] [endDate]"

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
COMMAND_HIVE="/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive "
IMK_NEW_PATH='/apps/b_marketing_tracking/delta_test/output_imk'
diff_file="${LOCAL_PATH}/verify/imk_diff_ts_${FILE_NAME}"
cd ${LOCAL_PATH}
pwd
echo > $diff_file;
while [[ $current_date -le $END_DATE ]]; do
    echo "current_date:$current_date";
    echo "begin deal with data in $current_date";
    dt=${current_date:0:4}-${current_date:4:2}-${current_date:6}
    echo "dt:$dt";
    sql_file="./tmp/count_diff_dt_and_ts_by_dt.sql";
    sed "s/#{dt}/${dt}/g" count_diff_dt_and_ts_by_dt_template.sql > $sql_file;
    cat $sql_file;
    $COMMAND_HIVE -f $sql_file;
    hive_result_code=$?
    echo "hive exit code: $hive_result_code"
    if [ $hive_result_code -ne 0 ]; then
        echo "hive fail:${dt}";
        exit $hive_result_code;
    fi
    count=$(cat ./diffTsCount/000000_0)
    echo "${dt}:${count}";
    echo "${dt}#${count}" >> $diff_file;
    current_date=$(date -d"${current_date} 1 days" +"%Y%m%d");
done