import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

dedupe_psearch_args = {
    'owner': 'chocolate',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['fechen@ebay.com','ganghuang@ebay.com','jialili1@ebay.com','xiangli4@ebay.com','yliu29@ebay.com','huiclu@ebay.com','yimeng@ebay.com','shuangxu@ebay.com','yiryuan@ebay.com','zhofan@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dedupe_psearch_dag = DAG(
    'dedupe_psearch_dag',
    default_args=dedupe_psearch_args,
    description='chocolate dedupe and sink job running every 5 mins',
    schedule_interval='*/5 * * * *')

dedupe_psearch_task = BashOperator(
    task_id='dedupe_psearch_task',
    bash_command='export HADOOP_USER_NAME=chocolate; /datashare/mkttracking/jobs/tracking/sparknrt/bin/prod/cappingRule.sh PAID_SEARCH /apps/tracking-events-workdir /apps/tracking-events /apps/tracking-events-archiveDir 150 http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200 ',
    execution_timeout=timedelta(minutes=1),
    dag=dedupe_psearch_dag)