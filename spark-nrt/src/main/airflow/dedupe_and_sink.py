from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow import DAG
from datetime import timedelta

dag_name = 'dedupe_and_sink_epn'
dag_id = 'dedupe_and_sink_epn'

default_args = {
    'owner': 'yuhxiao',
    'depends_on_past': False,
    'start_date': '2021-02-19',
    'email': ['yuhxiao@ebay.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval=None
)

"""
{
    "appName": "dedupe_and_sink_epn",
    "channel": "EPN",
    "kafkaTopic": "marketing.tracking.ssl.filtered-epn",
    "workDir": "viewfs://apollo-rno/user/b_marketing_tracking/tracking-events-workdir",
    "outputDir": "viewfs://apollo-rno/user/b_marketing_tracking/tracking-events",
    "partitions": "1",
    "couchbaseDedupe": false,
    "maxConsumeSize": 60000
}
"""

__config = {
    'name': dag_name,
    'java_class': 'com.ebay.traffic.chocolate.sparknrt.sink_v2.DedupeAndSink_v2',
    'application': '/mnt/jobs/tracking/spark-nrt/lib/chocolate-spark-nrt-*.jar',
    'executor_cores': 1,
    'driver_memory': '4G',
    'executor_memory': '6G',
    'num_executors': 20,

    # NBA params
    'application_args': [
        'channel',
        '--appName', '{{dag_run.conf["appName"]}}',
        '--channel', '{{dag_run.conf["channel"]}}',
        '--kafkaTopic', '{{dag_run.conf["kafkaTopic"]}}',
        '--workDir', '{{dag_run.conf["workDir"]}}',
        '--outputDir', '{{dag_run.conf["outputDir"]}}',
        '--partitions', '{{dag_run.conf["partitions"]}}',
        '--maxConsumeSize', '{{dag_run.conf["maxConsumeSize"]}}',
        '--couchbaseDedupe', '{{dag_run.conf["couchbaseDedupe"]}}',
        '--debug'
    ]
}

spark_submit_operator = SparkSubmitOperator(
    task_id='dedupe_and_sink_epn',
    pool='spark_pool',
    conn_id='spark-hdlq-commrce-product-high-mem',
    files='file:///mnt/jobs/tracking/spark-nrt/conf/dedupe_and_sink_v2.properties,'
          'file:///mnt/jobs/tracking/spark-nrt/conf/couchbase_v2.properties,'
          'file:///mnt/jobs/tracking/spark-nrt/conf/kafka_v2.properties,'
          'file:///mnt/jobs/tracking/spark-nrt/conf/sherlockio.properties,'
          'file:///mnt/apache/confs/hive/conf/hive-site.xml,'
          'file:///mnt/apache/confs/hadoop/conf/ssl-client.xml',
    conf={
        'spark.dynamicAllocation.maxExecutors': 80,
        'spark.ui.view.acls': '*',
        'spark.yarn.executor.memoryOverhead': 8192,
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.hadoop.yarn.timeline-service.enabled': 'false',
        'spark.sql.autoBroadcastJoinThreshold': 33554432,
        'spark.sql.shuffle.partitions': '200',
        'spark.speculation': 'false',
        'spark.yarn.maxAppAttempts': 3,
        'spark.driver.maxResultSize': '10g',
        'spark.kryoserializer.buffer.max': '2040m',
        'spark.eventLog.enabled': 'true',
        'spark.eventLog.compress': 'false',
        'spark.task.maxFailures': 3
    },
    dag=dag,
    **__config
)
