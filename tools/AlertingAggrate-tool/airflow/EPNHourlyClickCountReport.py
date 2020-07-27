import pendulum
import pytz
from airflow import DAG
from datetime import timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator

__author__ = "lxiong1"

default_args = {
    'owner': 'xiong1',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2020, 7, 13, tzinfo=pytz.timezone('America/Los_Angeles')),
    'email': ['DL-eBay-Chocolate-GC@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30)
}

remote_hosts = 'slcchocolatepits-1242736.stratus.slc.ebay.com'
ssh_conn_id = 'tracking-slc1242736'

dag = DAG(dag_id='EPNHourlyClickCountReport', schedule_interval='20 * * * *', default_args=default_args, catchup=False)

EPNHourlyClickCountReport = SSHOperator(task_id='EPNHourlyClickCountReport',
                                  ssh_conn_id=ssh_conn_id,
                                  remote_host=remote_hosts,
                                  command='/datashare/mkttracking/tools/AlertingAggrate-tool/bin/hourly_click_count_report.sh ',
                                  dag=dag)

EPNHourlyClickCountReport
