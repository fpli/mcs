#!/usr/bin/env bash
usage="Usage: TD_to_apollorno.sh]"

export HADOOP_USER_NAME=hdfs

DATE=`date --date="1 days ago" +%Y-%m-%d`
echo $DATE

table_name=$1
SQL=$2
td_type=$3

HDP=viewfs://apollo-rno/user/b_marketing_tracking/monitor/td/${table_name}_${td_type}
HDP_WITH_SUFFIX=${HDP}/lookup_

echo "hdp=$HDP"
echo "date=$DATE"
echo "table_name=$table_name"
echo "SQL=$SQL"
echo "td_type=$td_type"


echo "dumpFromTD started ~~~"
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -rm -r ${HDP}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -mkdir ${HDP}
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -chmod 777 ${HDP}

java -cp /datashare/mkttracking/tools/AlertingAggrate-tool/lib/Td-Bridge-*.jar com.ebay.traffic.chocolate.job.TdDataMoveMain sql=${SQL} td=${td_type} HDP=${HDP_WITH_SUFFIX} DT=$DATE TDUserName= TDPassWord=

rc=$?
if [[ $rc != 0 ]]; then
   echo "=====================================================dumpFromTD ERROR!!======================================================"
   exit $rc
else
   echo "dump lookup table data from TD done"
   echo "=====================================================dumpFromTD is completed======================================================"
fi

echo "merge small files started ~~~"
merge_dir=viewfs://apollo-rno/user/b_marketing_tracking/monitor/td/${table_name}_${td_type}_merge/
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -rm -r ${merge_dir}

DRIVER_MEMORY=2g
EXECUTOR_NUMBER=5
EXECUTOR_MEMORY=2g
EXECUTOR_CORES=1
SPARK_HOME=/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno

${SPARK_HOME}/bin/spark-submit \
    --class com.ebay.traffic.chocolate.job.MergeSmallFilesMain \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory ${DRIVER_MEMORY} \
    --num-executors ${EXECUTOR_NUMBER} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    --queue hdlq-commrce-product-high-mem \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.hadoop.yarn.timeline-service.enabled=false \
    --conf spark.sql.autoBroadcastJoinThreshold=33554432 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.speculation=false \
    --conf spark.yarn.maxAppAttempts=3 \
    --conf spark.driver.maxResultSize=10g \
    --conf spark.kryoserializer.buffer.max=2040m \
    --conf spark.task.maxFailures=3 \
    /datashare/mkttracking/tools/AlertingAggrate-tool/lib/Td-Bridge-*.jar yarn ${HDP} ${merge_dir}

rc=$?
if [[ $rc != 0 ]]; then
   echo "=====================================================merge small files ERROR!!======================================================"
   exit $rc
else
   echo "dump lookup table data from TD done"
   echo "=====================================================merge small files completed======================================================"
fi