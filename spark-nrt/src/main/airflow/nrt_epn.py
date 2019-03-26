import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

nrt_epn_args = {
    'owner': 'chocolate',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['fechen@ebay.com','ganghuang@ebay.com','jialili1@ebay.com','xiangli4@ebay.com','yliu29@ebay.com','huiclu@ebay.com','yimeng@ebay.com','shuangxu@ebay.com','yiryuan@ebay.com','zhofan@ebay.com'],
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

nrt_epn_dag = DAG(
    'nrt_epn_dag',
    default_args=nrt_epn_args,
    description='ePN nrt rule job running every 5 mins',
    schedule_interval='*/5 * * * *')

nrt_epn_task_1 = BashOperator(
    task_id='nrt_epn_task_1',
    bash_command='export HADOOP_USER_NAME=chocolate; /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/epnnrt-scheduler.sh ',
    execution_timeout=timedelta(minutes=5),
    dag=nrt_epn_dag)

nrt_epn_task_2 = BashOperator(
    task_id='nrt_epn_task_2',
    bash_command='/datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/epnnrt-send.sh ',
    execution_timeout=timedelta(minutes=5),
    dag=nrt_epn_dag)

nrt_epn_task_1.set_upstream(nrt_epn_task_2)