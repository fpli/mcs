import pendulum
import pytz
from airflow import DAG
from datetime import timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator

__author__ = "yli19"

default_args = {
    'owner': 'yli19',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2021, 1, 11, tzinfo=pytz.timezone('America/Los_Angeles')),
    'email': ['DL-eBay-Chocolate-GC@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30)
}

remote_hosts = 'slcchocolatepits-1242736.stratus.slc.ebay.com'
ssh_conn_id = 'tracking-slc1242736'

dag = DAG(dag_id='HourlyClickForIMK_v2', schedule_interval='50 * * * *', default_args=default_args, catchup=False)

HourlyClickForIMK_v2 = SSHOperator(task_id='HourlyClickForIMK_v2',
                                ssh_conn_id=ssh_conn_id,
                                remote_host=remote_hosts,
                                command='/datashare/mkttracking/tools/AlertingAggrate-tool-imk-v2/bin/hourly_click_count_report_imk_rno.sh ',
                                dag=dag)

HourlyClickForIMK_v2
