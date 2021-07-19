from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

__author__ = "yuhxiao"

default_args = {
    'owner': 'yuhxiao',
    'depends_on_past': False,
    'start_date': '2021-03-01',
    'email': ['yuhxiao@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='epnNrtClickAutomationTest',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)

auto_create_click_meta = BashOperator(
    dag=dag,
    bash_command='/datashare/mkttracking/jobs/tracking/epnnrt_new_test/bin/auto_create_click_meta_by_date_c3.sh ',
    task_id='auto_create_click_meta'
)

epnnrt_click_new_scheduler = SparkSubmitOperator(
    task_id='epnnrt_click_new_scheduler',
    pool='spark_pool',
    conn_id='hdlq-commrce-mkt-tracking-high-mem',
    files='file:///datashare/mkttracking/jobs/tracking/epnnrt_new_test/conf/epnnrt_v2.properties,'
          'file:///datashare/mkttracking/jobs/tracking/epnnrt_new_test/conf/sherlockio.properties,'
          'file:///datashare/mkttracking/jobs/tracking/epnnrt_new_test/conf/couchbase_v2.properties,'
          'file:///datashare/mkttracking/jobs/tracking/epnnrt_new_test/conf/df_epn_click_v2.json,'
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
    spark_binary="/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit",
    dag=dag,
    **{
        'name': "epnnrt_click_new_test_scheduler",
        'java_class': 'com.ebay.traffic.chocolate.sparknrt.epnnrtV2.EpnNrtClickJobV2',
        'application': '/datashare/mkttracking/jobs/tracking/epnnrt_new_test/lib/chocolate-spark-nrt-3.8.0-RELEASE-fat.jar',
        'executor_cores': '5',
        'driver_memory': '20G',
        'executor_memory': '40G',
        'num_executors': '40',
        'application_args': [
            '--appName', 'epn_nrt_click_new_test',
            '--mode', 'yarn',
            '--inputWorkDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir-new-test',
            '--outputWorkDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir-new-test',
            '--outputDir', 'viewfs://apollo-rno//apps/b_marketing_tracking/chocolate/epnnrt-new-test/',
            '--resourceDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-resources',
            '--partitions', '3',
            '--filterTime', '0'
        ]
    }
)
epnnrt_click_new_scheduler.set_upstream(auto_create_click_meta)

epnnrt_click_old_scheduler = SparkSubmitOperator(
    task_id='epnnrt_click_old_scheduler',
    pool='spark_pool',
    conn_id='hdlq-commrce-mkt-tracking-high-mem',
    files='file:///datashare/mkttracking/jobs/tracking/epnnrt_old_test/conf/epnnrt_v2.properties,'
          'file:///datashare/mkttracking/jobs/tracking/epnnrt_old_test/conf/sherlockio.properties,'
          'file:///datashare/mkttracking/jobs/tracking/epnnrt_old_test/conf/couchbase_v2.properties,'
          'file:///datashare/mkttracking/jobs/tracking/epnnrt_old_test/conf/df_epn_click_v2.json,'
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
    spark_binary="/datashare/mkttracking/tools/apollo_rno/spark_apollo_rno/bin/spark-submit",
    dag=dag,
    **{
        'name': "epnnrt_click_old_test_scheduler",
        'java_class': 'com.ebay.traffic.chocolate.sparknrt.epnnrtV2.EpnNrtClickJobV2',
        'application': '/datashare/mkttracking/jobs/tracking/epnnrt_old_test/lib/chocolate-spark-nrt-3.8.0-RELEASE-fat.jar',
        'executor_cores': '5',
        'driver_memory': '20G',
        'executor_memory': '40G',
        'num_executors': '40',
        'application_args': [
            '--appName', 'epn_nrt_click_old_test',
            '--mode', 'yarn',
            '--inputWorkDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir-old-test',
            '--outputWorkDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-events-workdir-old-test',
            '--outputDir', 'viewfs://apollo-rno//apps/b_marketing_tracking/chocolate/epnnrt-old-test/',
            '--resourceDir', 'viewfs://apollo-rno/apps/b_marketing_tracking/tracking-resources',
            '--partitions', '3',
            '--filterTime', '0'
        ]
    }
)
epnnrt_click_old_scheduler.set_upstream(auto_create_click_meta)

epnnrt_click_data_parity = BashOperator(
    dag=dag,
    bash_command='/datashare/mkttracking/jobs/tracking/epnnrt_new_test/bin/epnnrt_click_data_parity.sh ',
    task_id='epnnrt_click_data_parity'
)
epnnrt_click_data_parity.set_upstream(epnnrt_click_new_scheduler)
epnnrt_click_data_parity.set_upstream(epnnrt_click_old_scheduler)
