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
    'rotation_dump_to_td_hourly',
    default_args=default_args,
    description='hourly dump rotation info from CB to TD',
    schedule_interval='0 * * * *')

task_1 = BashOperator(
    task_id='task_dumpRotationToTD',
    bash_command='/datashare/mkttracking/jobs/rotation/bin/dumpRotationToTD.sh ',
    dag=dag)

task_2 = BashOperator(
    task_id='task_sendToETLHost',
    bash_command='/datashare/mkttracking/jobs/rotation/bin/sendToETLHost.sh ',
    dag=dag)

task_2.set_upstream(task_1)