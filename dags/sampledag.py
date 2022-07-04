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
) as dag:
 
show_date = BashOperator(
    task_id='print_date',
    bash_command='sleep 2',
    dag=dag)
 
data_processing_test = DataProcPySparkOperator(
    dag=dag,
    task_id='data_processing_test',     ##Change the name of the task_id according to task
    main='some_folder/code/hello_GCP.py', ## This is your data processing code with location
)
 
show_date >> data_processing_test
