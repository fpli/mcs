from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG
from datetime import timedelta

dag_name = 'epn_nrt_impression_v2'
dag_id = 'epn_nrt_impression_v2'

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
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1
)

__config = {
    'name': dag_name,
    'java_class': 'com.ebay.traffic.chocolate.sparknrt.epnnrtV2.EpnNrtImpressionJobV2',
    'application': '/datashare/mkttracking/jobs/tracking/epn-nrt/lib/chocolate-spark-nrt-3.8.0-RELEASE-fat.jar',
    'executor_cores': '8',
    'driver_memory': '15G',
    'executor_memory': '30G',
    'num_executors': '50',
    'application_args': [
        '--appName', 'epn_nrt_impression_v2',
        '--mode', 'yarn',
        '--inputWorkDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir',
        '--outputWorkDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir',
        '--outputDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt_v3',
        '--resourceDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-resources',
        '--partitions', '3',
        '--filterTime', '0'
    ]
}

spark_submit_operator = SparkSubmitOperator(
    task_id='epn_nrt_impression_v2',
    pool='spark_pool',
    conn_id='hdlq-commrce-mkt-tracking-high-mem',
    files='file:///datashare/mkttracking/jobs/tracking/epn-nrt/conf/epnnrt_v2.properties,'
          'file:///datashare/mkttracking/jobs/tracking/epn-nrt/conf/sherlockio.properties,'
          'file:///datashare/mkttracking/jobs/tracking/epn-nrt/conf/couchbase_v2.properties,'
          'file:///datashare/mkttracking/jobs/tracking/epn-nrt/conf/df_epn_impression_v2.json,'
          'file:///datashare/mkttracking/exports/apache/confs/hive/conf/hive-site.xml,'
          'file:///datashare/mkttracking/exports/apache/confs/hadoop/conf/ssl-client.xml',
    conf={
        'spark.dynamicAllocation.maxExecutors': '80',
        'spark.ui.view.acls': '*',
        'spark.yarn.executor.memoryOverhead': '8192',
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
    dag=dag,
    spark_binary="/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit",
    **__config
)
