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
    'email_on_retry': False
}

remote_hosts = 'slcchocolatepits-1242736.stratus.slc.ebay.com'
ssh_conn_id = 'tracking-slc1242736'

dag = DAG(dag_id='kill_long_run_job', schedule_interval='20 * * * *', default_args=default_args, catchup=False)

kill_long_run_job_for_UTPImkHourlyDoneHercules  = SSHOperator(task_id='kill_long_run_job',
                                ssh_conn_id=ssh_conn_id,
                                remote_host=remote_hosts,
                                command='/datashare/mkttracking/jobs/kill-long-run-job/bin/kill_long_run_job.sh "HERCULES" "com.ebay.traffic.chocolate.sparknrt.hourlyDone.UTPImkHourlyDoneJob" 60 ',
                                dag=dag)

kill_long_run_job_for_UTPImkHourlyDoneHercules
