#!/bin/bash
# run spark job on YARN
# Schedule: * * ? * *


bin=$(
  cd "$bin" >/dev/null
  pwd
)

JOB_NAME="EpnnrtClickDataParity"
click_dt='2021-04-09'
sql_file="./tmp/count_diff_ams_click_by_click_dt_${click_dt}.sql";
sed "s/#{click_dt}/${click_dt}/g" count_diff_ams_click_by_click_dt_template.sql > "$sql_file";

sql=`cat $sqlFile`;

/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit  \
--class com.ebay.traffic.chocolate.sparknrt.epnnrt.dataParity.EpnNrtDataParityJob \
--master yarn \
--deploy-mode cluster \
--queue hdlq-commrce-default \
--num-executors 160 \
--executor-memory 32G \
--executor-cores 8 \
${bin}/lib/chocolate-spark-nrt-3.6.1-RELEASE-fat.jar \
--sqlFile "${sql}"

spark_result_code=$?;
echo "spark_result_code:$spark_result_code";
if [ $spark_result_code -ne 0 ]; then
    echo "reformat data fail:${click_dt}";
    exit $spark_sql_result_code;
fi