from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator

dag_name = 'cb2nd'
dag_id = 'cb2nd'

default_args = {
    'owner': 'yuhxiao',
    'depends_on_past': False,
    'start_date': '2021-03-01',
    'email': ['Marketing-Tracking-oncall@ebay.com','DL-eBay-Chocolate-GC@ebay.com'],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=dag_id,
    schedule_interval='0 1 * * *',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)

__config = {
    'name': dag_name,
    'java_class': 'com.ebay.traffic.chocolate.sparknrt.cb2nd.CB2NDTool',
    'application': '/datashare/mkttracking/jobs/tracking/cb2nd/lib/chocolate-spark-nrt-3.8.0-RELEASE-fat.jar',
    'executor_cores': '1',
    'driver_memory': '10G',
    'executor_memory': '2G',
    'num_executors': '1',
    'application_args': [
        '--appName', 'CB2NDTool',
        '--mode', 'yarn',
        '--begin', '{{dag_run.conf.get("begin","")}}',
        '--end', '{{dag_run.conf.get("end","")}}'
    ]
}

syncData = SparkSubmitOperator(
    task_id='cb2nd',
    pool='spark_pool',
    conn_id='hdlq-commrce-mkt-tracking-high-mem',
    files='file:///datashare/mkttracking/jobs/tracking/cb2nd/conf/couchbase_v2.properties,'
          'file:///datashare/mkttracking/exports/apache/confs/hive/conf/hive-site.xml,'
          'file:///datashare/mkttracking/exports/apache/confs/hadoop/conf/ssl-client.xml',
    conf={
        'spark.master': 'yarn',
        'spark.yarn.am.waitTime': '60000000',
        'spark.network.timeout': '60000s',
        'spark.dynamicAllocation.maxExecutors': '80',
        'spark.ui.view.acls': '*',
        'spark.yarn.executor.memoryOverhead': '8192',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.hadoop.yarn.timeline-service.enabled': 'false',
        'spark.sql.autoBroadcastJoinThreshold': '33554432',
        'spark.sql.shuffle.partitions': '200',
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
touchNDDone = BashOperator(
    dag=dag,
    bash_command='/datashare/mkttracking/jobs/tracking/cb2nd/bin/rotation_done_daily_job.sh ',
    task_id='touchNDDone'
)
touchNDDone.set_upstream(syncData)