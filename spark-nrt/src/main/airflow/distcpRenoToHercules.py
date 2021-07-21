from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator

__author__ = "yuhxiao"

default_args = {
    'owner': 'yuhxiao',
    'depends_on_past': False,
    'start_date': '2021-03-01',
    'email': ['Marketing-Tracking-oncall@ebay.com','DL-eBay-Chocolate-GC@ebay.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='distcpRenoToHercules_v3',
    schedule_interval='*/10 * * * *',
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)

distcpRenoToHerculesClick = BashOperator(
    dag=dag,
    bash_command='/datashare/mkttracking/jobs/tracking/epn-nrt/bin/distcpAmsRenoToHercules_v3.sh /apps/b_marketing_tracking/chocolate/epnnrt_v2/click /sys/edw/imk/im_tracking/epn/ams_click_v2/snapshot click && '
                 '/datashare/mkttracking/jobs/tracking/epn-nrt/bin/distcpAmsRenoToHercules_v3.sh /apps/b_marketing_tracking/chocolate/epnnrt_v2/imp /sys/edw/imk/im_tracking/epn/ams_imprsn_v2/snapshot imp',
    task_id='distcpRenoToHerculesClick'
)

