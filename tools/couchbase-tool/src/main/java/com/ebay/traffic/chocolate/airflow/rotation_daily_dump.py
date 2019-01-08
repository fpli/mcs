import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

rotation_snapshot_daily_args = {
    'owner': 'rotation',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['DL-eBay-Chocolate-GC@ebay.com'],
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

rotation_snapshot_daily_task = BashOperator(
    task_id='rotation_snapshot_daily_task',
    bash_command='/datashare/mkttracking/jobs/rotation/bin/dumpRotationSnapshot.sh ',
    execution_timeout=timedelta(minutes=15),
    dag=rotation_snapshot_daily_dag)
