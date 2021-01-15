set -x
# count /apps/b_marketing_tracking/delta_test/output_imk files num
# FILE_NAME like 202001
# beginDate like 20200101
# endDate like 20200131
usage="Usage: imkReformatDataCheck.sh [beginDate] [endDate] [type]"

if [ $# -lt 3 ]; then
  echo $usage
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
    echo $usage;
    exit 1;
fi

current_date=$BEGIN_DATE;
echo "current_date:$current_date";

LOCAL_PATH='/datashare/mkttracking/jobs/imkReformate'
cd ${LOCAL_PATH}
pwd

while [[ $current_date -le $END_DATE ]]; do
    echo "current_date:$current_date";
    echo "begin deal with data in $current_date";
    dt=${current_date:0:4}-${current_date:4:2}-${current_date:6}
    echo "dt:$dt";
    if [ $TYPE == 'imk' ]; then
      sql_file="./tmp/count_diff_imk_and_new_by_dt_${dt}.sql";
      sed "s/#{dt}/${dt}/g" count_diff_imk_and_new_by_dt_template.sql > $sql_file;
    elif [ $TYPE == 'dtl' ]; then
      sql_file="./tmp/count_diff_dtl_and_new_by_dt_${dt}.sql";
      sed "s/#{dt}/${dt}/g" count_diff_dtl_and_new_by_dt_template.sql > $sql_file;
    else
      exit 1;
    fi
    cat $sql_file;
    ./imkReformat.sh $sql_file;
    spark_result_code=$?
    echo "spark exit code: $spark_result_code"
    if [ $spark_result_code -ne 0 ]; then
        echo "reformate data fail:${dt}";
        exit $spark_result_code;
    fi
    current_date=$(date -d"${current_date} 1 days" +"%Y%m%d");
done