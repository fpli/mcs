#!/bin/bash
set -x

CLUSTER=${1}
TABLE=${2}
PARTITION_NAME=${3}
PARTITION=${4}
PARTITION_NUM=${5}
OUTPUT_DIR=${6}

if [[ -z $CLUSTER ||  -z $TABLE || -z $PARTITION_NAME || -z $PARTITION || $PARTITIO_NUM  || -z $OUTPUT_DIR ]]
then
    echo $usage;
    exit 1;
fi

if [ "${CLUSTER}" == "rno" ]
then
    HDFS_PATH="/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs"
    HIVE_PATH="/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive"
    SPARK_PATH="/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit"
    QUEUE="hdlq-commrce-mkt-tracking-high-mem"
elif [ "${CLUSTER}" == "hercules" ]
then
    HDFS_PATH="/datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hdfs"
    HIVE_PATH="/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive"
    SPARK_PATH="/datashare/mkttracking/tools/hercules_lvs/spark-hercules/bin/spark-submit"
    QUEUE="hdlq-data-batch-low"
else
    echo "Wrong cluster to merge data!"
    exit 1
fi

bin=$(dirname "$0")
bin=$(
  cd "$bin" >/dev/null
  pwd
)


${SPARK_PATH} \
--class com.ebay.traffic.chocolate.sparknrt.mergeSmallFiles.MergeSmallFilesJob \
--master yarn \
--deploy-mode cluster \
--queue ${QUEUE} \
--num-executors 120 \
--executor-memory 16G \
--executor-cores 8 \
${bin}/../lib/chocolate-spark-nrt-*.jar \
--table ${TABLE} \
--partitionName ${PARTITION_NAME} \
--partition ${PARTITION} \
--partitionNum ${PARTITION_NUM} \
--outputDir ${OUTPUT_DIR}
