from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator

__author__ = "yli19"

default_args = {
    'owner': 'yli19',
    'depends_on_past': False,
    'start_date': '2021-07-07',
    'email': ['DL-eBay-Chocolate-GC@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='kill_long_run_job',
    schedule_interval='20 * * * *',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)

kill_long_run_job = BashOperator(
        dag=dag,
        bash_command='/datashare/mkttracking/jobs/kill-long-run-job/bin/kill_long_run_job.sh "HERCULES" "com.ebay.traffic.chocolate.sparknrt.hourlyDone.UTPImkHourlyDoneJob" 60 ',
        task_id='kill_long_run_job'
)