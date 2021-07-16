from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator

__author__ = "yuhxiao"

default_args = {
    'owner': 'yuhxiao',
    'depends_on_past': False,
    'start_date': '2021-03-01',
    'email': ['Marketing-Tracking-oncall@ebay.com','DL-eBay-Chocolate-GC@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='epnNrtimprsnAutomationTest',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)

auto_create_imprsn_meta = BashOperator(
    dag=dag,
    bash_command='/datashare/mkttracking/jobs/tracking/epnnrt_new_test/bin/auto_create_imprsn_meta_by_date_c3.sh ',
    task_id='auto_create_imprsn_meta'
)

epnnrt_imprsn_new_scheduler = BashOperator(
    dag=dag,
    bash_command='/datashare/mkttracking/jobs/tracking/epnnrt_new_test/bin/epnnrt_imprsn_new_test-scheduler.sh ',
    task_id='epnnrt_imprsn_new_scheduler'
)
epnnrt_imprsn_new_scheduler.set_upstream(auto_create_imprsn_meta)

epnnrt_imprsn_old_scheduler = BashOperator(
    dag=dag,
    bash_command='/datashare/mkttracking/jobs/tracking/epnnrt_new_test/bin/epnnrt_imprsn_old_test-scheduler.sh ',
    task_id='epnnrt_imprsn_old_scheduler'
)
epnnrt_imprsn_old_scheduler.set_upstream(auto_create_imprsn_meta)

epnnrt_imprsn_data_parity = BashOperator(
    dag=dag,
    bash_command='/datashare/mkttracking/jobs/tracking/epnnrt_new_test/bin/epnnrt_imprsn_data_parity.sh ',
    task_id='epnnrt_imprsn_data_parity'
)
epnnrt_imprsn_data_parity.set_upstream(epnnrt_imprsn_new_scheduler)
epnnrt_imprsn_data_parity.set_upstream(epnnrt_imprsn_old_scheduler)
