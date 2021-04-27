#!/bin/bash
# run spark job on YARN
# Schedule: * * ? * *


bin=$(
  cd "$bin" >/dev/null
  pwd
)

JOB_NAME="EpnnrtImpressionAutomationParity"
imprsn_dt=`date -d '5 days ago' +%Y-%m-%d`
sql_file="./tmp/count_diff_ams_imprsn_by_imprsn_dt_${imprsn_dt}.sql";
sed "s/#{imprsn_dt}/${imprsn_dt}/g" count_diff_ams_imprsn_by_imprsn_dt_template.sql > "$sql_file";

sql=`cat $sqlFile`;

/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit  \
--class com.ebay.traffic.chocolate.sparknrt.epnnrt_v2.dataParity.EpnNrtDataParityJob \
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
    echo "reformat data fail:${imprsn_dt}";
    exit $spark_sql_result_code;
fi