import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

dedupe_psearch_args = {
    'owner': 'chocolate',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['yimeng@ebay.com'],
    'email_on_failure': False,
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
    bash_command='export HADOOP_USER_NAME=chocolate; ./dedupeAndSink.sh PAID_SEARCH marketingtech.ap.tracking-events.filtered-paid-search /apps/tracking-events-workdir /apps/tracking-events http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200 1 true ',
    execution_timeout=timedelta(minutes=5),
    dag=dedupe_psearch_dag)

