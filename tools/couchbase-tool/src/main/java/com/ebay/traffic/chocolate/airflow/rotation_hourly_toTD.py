import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

rotation_td_hourly_args = {
    'owner': 'rotation',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['fechen@ebay.com','ganghuang@ebay.com','jialili1@ebay.com','xiangli4@ebay.com','yliu29@ebay.com','huiclu@ebay.com','yimeng@ebay.com','shuangxu@ebay.com','yiryuan@ebay.com','zhofan@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

rotation_td_hourly_dag = DAG(
    'rotation_td_hourly_dag',
    default_args=rotation_td_hourly_args,
    description='hourly dump rotation info from CB to TD',
    schedule_interval='@hourly')

rotation_td_hourly_task_1 = BashOperator(
    task_id='task_dumpRotationToTD',
    bash_command='/datashare/mkttracking/jobs/rotation/bin/dumpRotationToTD.sh ',
    execution_timeout=timedelta(minutes=10),
    dag=rotation_td_hourly_dag)

rotation_td_hourly_task_2 = BashOperator(
    task_id='task_sendToETLHost',
    bash_command='/datashare/mkttracking/jobs/rotation/bin/sendToETLHost.sh ',
    execution_timeout=timedelta(minutes=5),
    dag=rotation_td_hourly_dag)

rotation_td_hourly_task_2.set_upstream(rotation_td_hourly_task_1)