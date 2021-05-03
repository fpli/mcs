#!/bin/bash
# run spark job on YARN
# Schedule: * * ? * *


bin=$(
  cd "$bin" >/dev/null
  pwd
)

JOB_NAME="EpnnrtClickAutomationParity"
click_dt=`date -d '5 days ago' +%Y-%m-%d`
sql_file="../../sql/tmp/count_diff_epnnrt_automation_by_click_dt_${click_dt}.sql";
sed "s/#{click_dt}/${click_dt}/g" ../../sql/count_diff_epnnrt_automation_by_click_dt_template.sql > "$sql_file";

sql=`cat $sql_file`;
echo "$sql"
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -rm -r /apps/b_marketing_tracking/epnnrt-automation-diff/click/*

/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit  \
--class com.ebay.traffic.chocolate.sparknrt.imkReformat.ImkReformatJob \
--master yarn \
--deploy-mode cluster \
--queue hdlq-commrce-mkt-tracking-high-mem \
--num-executors 160 \
--executor-memory 32G \
--executor-cores 8 \
${bin}/../../lib/chocolate-spark-nrt-*.jar \
--sqlFile "${sql}"

spark_result_code=$?;
echo "spark_result_code:$spark_result_code";
if [ $spark_result_code -ne 0 ]; then
    echo "data parity fail:${click_dt}";
    exit $spark_sql_result_code;
fi
