#!/bin/bash
# run spark job on YARN
# Schedule: * * ? * *


bin=$(
  cd "$bin" >/dev/null
  pwd
)

JOB_NAME="EpnnrtImpressionAutomationParity"
imprsn_dt=`date -d '5 days ago' +%Y-%m-%d`
sql_file="../../sql/tmp/count_diff_ams_imprsn_by_imprsn_dt_${imprsn_dt}.sql";
sed "s/#{imprsn_dt}/${imprsn_dt}/g" ../../sql/count_diff_epnnrt_automation_by_imprsn_dt_template.sql > "$sql_file";

sql=`cat $sqlFile`;

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -rm -r /apps/b_marketing_tracking/epnnrt-automation-diff/imp/*

/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit  \
--class com.ebay.traffic.chocolate.sparknrt.imkReformat.ImkReformatJob \
--master yarn \
--deploy-mode cluster \
--queue hdlq-commrce-mkt-tracking-high-mem\
--num-executors 160 \
--executor-memory 32G \
--executor-cores 8 \
${bin}/../../lib/chocolate-spark-nrt-*.jar \
--sqlFile "${sql}"

spark_result_code=$?;
echo "spark_result_code:$spark_result_code";
if [ $spark_result_code -ne 0 ]; then
    echo "data parity fail:${imprsn_dt}";
    exit $spark_sql_result_code;
fi