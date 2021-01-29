set -x
# count /apps/b_marketing_tracking/delta_test/output_ams files num
# FILE_NAME like 202001
# beginDate like 20200101
# endDate like 20200131

usage="Usage: amsimprsnReformatCheck.sh [beginDate] [endDate]"

if [ $# -lt 2 ]; then
  echo "$usage"
  exit 1
fi

BEGIN_DATE=${1}
END_DATE=${2}
TYPE=${3}

echo "BEGIN_DATE:$BEGIN_DATE";
echo "END_DATE:$END_DATE";
echo "TYPE:$TYPE";

if [[ -z $BEGIN_DATE ||  -z $END_DATE || $END_DATE < $BEGIN_DATE ]]
then
    echo "$usage";
    exit 1;
fi

current_date=$BEGIN_DATE;
echo "current_date:$current_date";

LOCAL_PATH='/datashare/mkttracking/jobs/amsReformat/imprsn'
# shellcheck disable=SC2164
cd ${LOCAL_PATH}
pwd
while [[ $current_date -le $END_DATE ]]; do
    echo "current_date:$current_date";
    echo "begin deal with data in $current_date";
    imprsn_dt=${current_date:0:4}-${current_date:4:2}-${current_date:6}
    echo "imprsn_dt:$imprsn_dt";
    sql_file="./tmp/count_diff_ams_imprsn_by_imprsn_dt_${imprsn_dt}.sql";
    sed "s/#{imprsn_dt}/${imprsn_dt}/g" count_diff_ams_imprsn_by_imprsn_dt_template.sql > "$sql_file";
    cat "$sql_file";
    ../amsReformat.sh "$sql_file";
    spark_result_code=$?
    echo "spark exit code: $spark_result_code"
    if [ $spark_result_code -ne 0 ]; then
        echo "reformat data fail:${imprsn_dt}";
        exit $spark_result_code;
    fi
    current_date=$(date -d"${current_date} 1 days" +"%Y%m%d");
done