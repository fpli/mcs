from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from datetime import timedelta

dag_name = 'monitoring_epn_v2'
dag_id = 'monitoring_epn_v2'

default_args = {
    'owner': 'yuhxiao',
    'start_date': '2021-03-01',
    'email': ['yuhxiao@ebay.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1
)

__config = {
    'name': dag_name,
    'java_class': 'com.ebay.traffic.chocolate.sparknrt.monitoring_v2.MonitoringJob_v2',
    'application': '/datashare/mkttracking/jobs/tracking/spark-nrt/lib/chocolate-spark-nrt-3.8.0-RELEASE-fat.jar',
    'executor_cores': '1',
    'driver_memory': '4G',
    'executor_memory': '4G',
    'num_executors': '20',


    'application_args': [
        '--appName', 'monitoring_epn_v2',
        '--channel', 'EPN',
        '--workDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir'
    ]
}

spark_submit_operator = SparkSubmitOperator(
    task_id='monitoring_epn_v2',
    pool='spark_pool',
    conn_id='hdlq-commrce-mkt-tracking-high-mem',
    files='file:///datashare/mkttracking/jobs/tracking/spark-nrt/conf/sherlockio.properties,'
          'file:///datashare/mkttracking/exports/apache/confs/hive/conf/hive-site.xml,'
          'file:///datashare/mkttracking/exports/apache/confs/hadoop/conf/ssl-client.xml',
    conf={
        'spark.dynamicAllocation.maxExecutors': '80',
        'spark.ui.view.acls': '*',
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
    spark_binary="/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit",
    dag=dag,
    **__config
)