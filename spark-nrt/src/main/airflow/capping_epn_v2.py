from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from datetime import timedelta

dag_name = 'capping_epn_v2'
dag_id = 'capping_epn_v2'

default_args = {
    'owner': 'yuhxiao',
    'start_date': '2021-03-01',
    'email': ['Marketing-Tracking-oncall@ebay.com','DL-eBay-Chocolate-GC@ebay.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

dag = DAG(
    dag_id=dag_id,
    schedule_interval='*/4 * * * *',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)

__config = {
    'name': dag_name,
    'java_class': 'com.ebay.traffic.chocolate.sparknrt.cappingV2.CappingRuleJobV2',
    'application': '/datashare/mkttracking/jobs/tracking/spark-nrt/lib/chocolate-spark-nrt-3.8.0-RELEASE-fat.jar',
    'executor_cores': '2',
    'driver_memory': '6G',
    'executor_memory': '25G',
    'num_executors': '30',


    'application_args': [
        '--appName', 'capping_epn_v2',
        '--channel', 'EPN',
        '--workDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir',
        '--outputDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events',
        '--archiveDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-archiveDir',
        '--partitions', '3'
    ]
}

spark_submit_operator = SparkSubmitOperator(
    task_id='capping_epn_v2',
    pool='spark_pool',
    conn_id='hdlq-commrce-mkt-tracking-high-mem',
    files='file:///datashare/mkttracking/jobs/tracking/spark-nrt/conf/capping_rule_v2.properties,'
          'file:///datashare/mkttracking/exports/apache/confs/hive/conf/hive-site.xml,'
          'file:///datashare/mkttracking/exports/apache/confs/hadoop/conf/ssl-client.xml',
    conf={
        'spark.dynamicAllocation.maxExecutors': '80',
        'spark.ui.view.acls': '*',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.hadoop.yarn.timeline-service.enabled': 'false',
        'spark.sql.autoBroadcastJoinThreshold': '33554432',
        'spark.sql.shuffle.partitions': '200',
        'spark.speculation': 'true',
        'spark.speculation.quantile': '0.5',
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