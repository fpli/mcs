from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator

__author__ = "yuhxiao"

default_args = {
    'owner': 'yuhxiao',
    'depends_on_past': False,
    'start_date': '2021-03-01',
    #'email': ['DL-eBay-Chocolate-GC@ebay.com'],
    'email': ['yuhxiao@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='mvLocalAmsDelayData',
    schedule_interval='*/50 * * * *',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)

mvLocalAmsDelayData = BashOperator(
    dag=dag,
    bash_command='/datashare/mkttracking/jobs/tracking/epn-nrt/bin/mvLocalAmsDelayData.sh',
    task_id='mvLocalAmsDelayData'
)


