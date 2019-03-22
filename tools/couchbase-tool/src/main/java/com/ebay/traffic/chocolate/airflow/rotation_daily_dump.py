import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

rotation_snapshot_daily_args = {
    'owner': 'rotation',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['fechen@ebay.com','ganghuang@ebay.com','jialili1@ebay.com','xiangli4@ebay.com','yliu29@ebay.com','huiclu@ebay.com','yimeng@ebay.com','shuangxu@ebay.com','yiryuan@ebay.com','zhofan@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

rotation_snapshot_daily_dag = DAG(
    'rotation_snapshot_daily_dag',
    default_args=rotation_snapshot_daily_args,
    description='daily dump all rotation info from CB',
    schedule_interval='10 0 * * *',
    catchup=False)

rotation_snapshot_daily_task_1 = BashOperator(
    task_id='rotation_snapshot_daily_task_1',
    bash_command='/datashare/mkttracking/jobs/rotation/bin/dumpRotationSnapshot.sh ',
    execution_timeout=timedelta(minutes=15),
    dag=rotation_snapshot_daily_dag)

rotation_snapshot_daily_task_2 = BashOperator(
    task_id='rotation_snapshot_daily_task_2',
    bash_command='/datashare/mkttracking/jobs/rotation/bin/rotation_hive_daily_job.sh ',
    execution_timeout=timedelta(minutes=15),
    dag=rotation_snapshot_daily_dag)

rotation_snapshot_daily_task_2.set_upstream(rotation_snapshot_daily_task_1)
