import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")


DEFAULT_ARGS = {
    "owner": "db-volt",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Sample dag for unit testing",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=5),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["dag test demo"],
)  as dag:

    t1= BashOperator(
        task_id='echo',
        bash_command= 'echo test',
        dag=dag,
        depends_on_past=False,
        priority_weight=2**31-1)
        
