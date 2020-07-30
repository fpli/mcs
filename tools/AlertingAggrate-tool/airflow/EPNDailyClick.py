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

dag = DAG(dag_id='EPNDailyClick', schedule_interval='40 2 * * *', default_args=default_args, catchup=False)

DailyClickTrendReport = SSHOperator(task_id='DailyClickTrendReport',
                                  ssh_conn_id=ssh_conn_id,
                                  remote_host=remote_hosts,
                                  command='/datashare/mkttracking/tools/AlertingAggrate-tool/bin/daily_click_trend_report.sh ',
                                  dag=dag)

DailyDomainTrendReport = SSHOperator(task_id='DailyDomainTrendReport',
                                    ssh_conn_id=ssh_conn_id,
                                    remote_host=remote_hosts,
                                    command='/datashare/mkttracking/tools/AlertingAggrate-tool/bin/daily_domain_trend_report.sh ',
                                    dag=dag)


DailyClickTrendReport >> DailyDomainTrendReport