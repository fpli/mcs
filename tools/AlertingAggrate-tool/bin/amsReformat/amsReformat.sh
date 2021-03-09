#!/bin/bash
# run spark job on YARN
# Schedule: * * ? * *

bin=$(dirname "$0")
sqlFile=${1}
echo "bin:$bin";
echo "sqlFile path:$sqlFile";

bin=$(
  cd "$bin" >/dev/null
  pwd
)

sql=`cat $sqlFile`;
echo $sql;

if [[ $? -ne 0 ]]; then
  echo "get latest path failed"
  exit 1
fi

JOB_NAME="EpnReformat"

/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit  \
--class com.ebay.traffic.chocolate.sparknrt.imkReformat.ImkReformatJob \
--master yarn \
--deploy-mode cluster \
--queue hdlq-commrce-default \
--num-executors 160 \
--executor-memory 32G \
--executor-cores 8 \
${bin}/lib/chocolate-spark-nrt-3.6.1-RELEASE-fat-imk.jar \
--sqlFile "${sql}"

spark_result_code=$?;
echo "spark_result_code:$spark_result_code";
if [ $spark_result_code -ne 0 ]; then
    echo "reformat data fail:${click_dt}";
    exit $spark_sql_result_code;
fi