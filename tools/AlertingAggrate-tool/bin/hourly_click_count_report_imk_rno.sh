#!/usr/bin/env bash

DATE=`date --date= +%Y-%m-%d`
echo "Date: ${DATE}"

DRIVER_MEMORY=6g
EXECUTOR_NUMBER=40
EXECUTOR_MEMORY=8g
EXECUTOR_CORES=5
SPARK_HOME=/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno
FILES=/datashare/mkttracking/tools/AlertingAggrate-tool-imk-v2/conf/df_imk_new_apollo.json,/datashare/mkttracking/tools/AlertingAggrate-tool-imk-v2/conf/IMKClickReport.properties
inputdir=viewfs://apollo-rno/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_v2/*/
echo "inputdir: ${inputdir}"
outputdir=viewfs://apollo-rno/apps/b_marketing_tracking/alert/imk_v2/temp/hourlyClickCount
echo "outputdir: ${outputdir}"
jobtask=hourlyClickCount
echo "jobtask: ${jobtask}"
schema_imk_click_dir=df_imk_new_apollo.json
echo "schema_imk_click_dir: ${schema_imk_click_dir}"
header=false
echo "header: ${header}"

/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -mkdir -p viewfs://apollo-rno/apps/b_marketing_tracking/alert/imk_v2/temp/
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -rm -r viewfs://apollo-rno/apps/b_marketing_tracking/alert/imk_v2/temp/hourlyClickCount

${SPARK_HOME}/bin/spark-submit  \
    --files ${FILES}  \
    --class com.ebay.traffic.chocolate.job.IMKClickReport \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    --queue hdlq-commrce-default \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.hadoop.yarn.timeline-service.enabled=false \
    --conf spark.sql.autoBroadcastJoinThreshold=33554432 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.speculation=false \
    --conf spark.yarn.maxAppAttempts=3 \
    --conf spark.driver.maxResultSize=10g \
    --conf spark.kryoserializer.buffer.max=2040m \
    --conf spark.task.maxFailures=3 \
    /datashare/mkttracking/tools/AlertingAggrate-tool-imk-v2/lib/AlertingAggrate-tool-*.jar ${inputdir} ${outputdir} ${jobtask} ${schema_imk_click_dir} yarn ${DATE} ${header}