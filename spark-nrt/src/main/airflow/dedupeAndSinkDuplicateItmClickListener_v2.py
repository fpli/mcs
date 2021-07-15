from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta
from airflow import DAG

dag_name = 'dedupeAndSinkDuplicateItmClickListener_v2.py'
dag_id = 'dedupeAndSinkDuplicateItmClickListener_v2.py'

default_args = {
    'owner': 'yuhxiao',
    'start_date': '2021-02-19',
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
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)

__config = {
    'name': dag_name,
    'java_class': 'com.ebay.traffic.chocolate.sparknrt.sinkListenerV2.DedupeAndSinkListenerV2',
    'application': '/datashare/mkttracking/jobs/tracking/spark-nrt/lib/chocolate-spark-nrt-3.8.0-RELEASE-fat.jar',
    'executor_cores': '1',
    'driver_memory': '4G',
    'executor_memory': '6G',
    'num_executors': '20',

    'application_args': [
        '--appName', 'dedupeAndSinkDuplicateItmClickListener_v2.py',
        '--channel', 'DUPLICATE_CLICK_LISTENER',
        '--kafkaTopic', 'marketing.tracking.ssl.listened-duplicate-itm-click',
        '--workDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir-duplicate-click',
        '--outputDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-duplicate-click',
        '--partitions', '1',
        '--maxConsumeSize', '60000',
        '--couchbaseDedupe', 'true'
    ]
}

spark_submit_operator = SparkSubmitOperator(
    task_id='dedupeAndSinkDuplicateItmClickListener_v2.py',
    pool='spark_pool',
    conn_id='hdlq-commrce-mkt-tracking-high-mem',
    files='file:///datashare/mkttracking/jobs/tracking/spark-nrt/conf/dedupe_and_sink_v2.properties,'
          'file:///datashare/mkttracking/jobs/tracking/spark-nrt/conf/couchbase_v2.properties,'
          'file:///datashare/mkttracking/jobs/tracking/spark-nrt/conf/kafka-listener_v2.properties,'
          'file:///datashare/mkttracking/jobs/tracking/spark-nrt/conf/sherlockio.properties,'
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
        'spark.task.maxFailures': '3',
        'spark.executorEnv.TRF_GRANT_FILE': '/ebay/trustfabric.cg',
        'spark.executorEnv.CHECK_TF_TOKEN_IN_FOUNT': 'true',
        'spark.executorEnv.APP_INSTANCE_NAME': 'default-appinstance',
        'spark.executorEnv.APP_NAME': 'hadoopapollorno',
        'spark.executorEnv.APP_ENV': 'production',
        'spark.executorEnv.TRF_ENABLE_V2': 'true',
        'spark.yarn.appMasterEnv.TRF_GRANT_FILE': '/ebay/trustfabric.cg',
        'spark.yarn.appMasterEnv.CHECK_TF_TOKEN_IN_FOUNT': 'true',
        'spark.yarn.appMasterEnv.APP_INSTANCE_NAME': 'default-appinstance',
        'spark.yarn.appMasterEnv.APP_NAME': 'hadoopapollorno',
        'spark.yarn.appMasterEnv.APP_ENV': 'production',
        'spark.yarn.appMasterEnv.TRF_ENABLE_V2': 'true'
    },
    spark_binary="/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit",
    dag=dag,
    **__config
)
