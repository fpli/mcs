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
    dag_id='deleteMeta',
    schedule_interval='20 5 * * *',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)

deleteMeta = BashOperator(
    dag=dag,
    bash_command='/datashare/mkttracking/jobs/tracking/epn-nrt/bin/deleteMeta.sh ',
    task_id='deleteMeta'
)

