from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow import DAG
from datetime import timedelta

dag_name = 'epn_nrt_click_v2'
dag_id = 'epn_nrt_click_v2'

default_args = {
    'owner': 'yuhxiao',
    'depends_on_past': False,
    'start_date': '2021-03-01',
    'email': ['yuhxiao@ebay.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': '0',
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval=None
)

__config = {
    'name': dag_name,
    'java_class': 'com.ebay.traffic.chocolate.sparknrt.epnnrt_v2.EpnNrtClickJob_v2',
    'application': '/mnt/jobs/tracking/epn-nrt/lib/chocolate-spark-nrt-*.jar',
    'executor_cores': '1',
    'driver_memory': '4G',
    'executor_memory': '6G',
    'num_executors': '20',

    'application_args': [
        '--appName', 'epn_nrt_click_v2',
        '--mode', 'yarn',
        '--workDir', 'viewfs://apollo-rno/user/b_marketing_tracking/tracking-events-workdir',
        '--outputDir', 'viewfs://apollo-rno/user/b_marketing_tracking/chocolate/epnnrt_v2',
        '--resourceDir', 'viewfs://apollo-rno/user/b_marketing_tracking/tracking-resources',
        '--partitions', '3',
        '--filterTime', '0'
    ]
}

spark_submit_operator = SparkSubmitOperator(
    task_id='epn_nrt_click_v2',
    pool='spark_pool',
    conn_id='hdlq-commrce-mkt-high-mem',
    files='file:///mnt/jobs/tracking/epn-nrt/conf/epnnrt_v2.properties,'
          'file:///mnt/jobs/tracking/epn-nrt/conf/sherlockio.properties,'
          'file:///mnt/jobs/tracking/epn-nrt/conf/couchbase_v2.properties,'
          'file:///mnt/exports/apache/confs/hive/conf/hive-site.xml,'
          'file:///mnt/exports/apache/confs/hadoop/conf/ssl-client.xml',
    conf={
        'spark.dynamicAllocation.maxExecutors': '80',
        'spark.ui.view.acls': '*',
        'spark.yarn.executor.memoryOverhead': '8192',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.hadoop.yarn.timeline-service.enabled': 'false',
        'spark.sql.autoBroadcastJoinThreshold': '33554432',
        'spark.sql.shuffle.partitions': '200',
        'spark.speculation': 'false',
        'spark.yarn.maxAppAttempts': '3',
        'spark.driver.maxResultSize': '10g',
        'spark.kryoserializer.buffer.max': '2040m',
        'spark.eventLog.enabled': 'true',
        'spark.eventLog.compress': 'false',
        'spark.task.maxFailures': '3'
    },
    dag=dag,
    **__config
)
