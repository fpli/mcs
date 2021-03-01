from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow import DAG
from datetime import timedelta

dag_name = 'capping_epn_v2'
dag_id = 'capping_epn_v2'

default_args = {
    'owner': 'yuhxiao',
    'depends_on_past': False,
    'start_date': '2021-03-01',
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
    "appName": "capping_epn_v2",
    "channel": "EPN",
    "workDir": "viewfs://apollo-rno/user/b_marketing_tracking/tracking-events-workdir",
    "outputDir": "viewfs://apollo-rno/user/b_marketing_tracking/tracking-events",
    "archiveDir": "viewfs://apollo-rno/user/b_marketing_tracking/tracking-events-archiveDir",
    "partitions": "1"
    }
"""

__config = {
    'name': dag_name,
    'java_class': 'com.ebay.traffic.chocolate.sparknrt.capping_v2.CappingRuleJob_v2',
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
        '--workDir', '{{dag_run.conf["workDir"]}}',
        '--outputDir', '{{dag_run.conf["outputDir"]}}',
        '--archiveDir', '{{dag_run.conf["archiveDir"]}}',
        '--partitions', '{{dag_run.conf["partitions"]}}',
        '--debug'
    ]
}

spark_submit_operator = SparkSubmitOperator(
    task_id='capping_epn_v2',
    pool='default_pool',
    conn_id='hdlq-commrce-mkt-high-mem',
    files='file:///mnt/jobs/tracking/spark-nrt/conf/capping_rule_v2.properties,'
          'file:///apache/confs/hive/conf/hive-site.xml,'
          'file:///apache/confs/hadoop/conf/ssl-client.xml',
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
