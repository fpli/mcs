import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

capping_epn_args = {
    'owner': 'chocolate',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['fechen@ebay.com','ganghuang@ebay.com','jialili1@ebay.com','xiangli4@ebay.com','yliu29@ebay.com','huiclu@ebay.com','yimeng@ebay.com','shuangxu@ebay.com','yiryuan@ebay.com','zhofan@ebay.com'],
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

capping_epn_dag = DAG(
    'capping_epn_dag',
    default_args=capping_epn_args,
    description='chocolate capping rule job running every 5 mins',
    schedule_interval='*/5 * * * *')

capping_epn_task = BashOperator(
    task_id='capping_epn_task',
    bash_command='export HADOOP_USER_NAME=chocolate; ./cappingRule.sh EPN /apps/tracking-events-workdir /apps/tracking-events /apps/tracking-events-archiveDir 150 http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200 ',
    execution_timeout=timedelta(minutes=5),
    dag=capping_epn_dag)

