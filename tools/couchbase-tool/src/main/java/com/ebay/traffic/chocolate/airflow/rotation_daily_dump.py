import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
    'owner': 'rotation',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['yimeng@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'rotation_dump_snapshot_daily',
    default_args=default_args,
    description='daily dump all rotation info from CB',
    schedule_interval='0 0 * * *')

task = BashOperator(
    task_id='task_dumpRotationSnapshot',
    bash_command='/datashare/mkttracking/jobs/rotation/bin/dumpRotationSnapshot.sh ',
    dag=dag)