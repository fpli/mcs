import pendulum
import pytz
from airflow import DAG
from datetime import timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator

__author__ = "yuhxiao"

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

dag = DAG(dag_id='AlertingAggregation_v2', schedule_interval='0 5 * * *', default_args=default_args, catchup=False)

AlertingAggregation_v2 = SSHOperator(task_id='AlertingAggregation_v3',
                                  ssh_conn_id=ssh_conn_id,
                                  remote_host=remote_hosts,
                                  command='/datashare/mkttracking/tools/AlertingAggrate-tool-imk-v2/bin/alerting_aggregation_v3.sh ',
                                  dag=dag)

AlertingAggregation_v2
