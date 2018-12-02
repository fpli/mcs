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
    'rotation_dump_to_hive_hourly',
    default_args=default_args,
    description='hourly dump rotation info from CB to TD',
    schedule_interval='0 * * * *')

task = BashOperator(
    task_id='task_dumpRotationToHive',
    bash_command='/datashare/mkttracking/jobs/rotation/bin/dumpRotationToHadoop.sh ',
    dag=dag)