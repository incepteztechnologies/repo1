from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago, timedelta,datetime

location="us-central1"
arg1='gs://incpetez-data-samples/dataset/bqdata/ext_src_data/custs_header_20230908'
arg2='incr'

default_args = {"owner": "Inceptez Tech",
                "start_date": datetime(2023, 9, 16),#From when this DAG has to start scheduled start_date=days_ago(1)
                "email": "info@inceptez.com",
                "email_on_failure": True,
                "email_on_retry": True,
                "retries": 1,#How many retries if this DAG failed due to a task failure
                "retry_delay": timedelta(minutes=1),#How long to wait for next retry after the failure
                "schedule_interval": "@daily",#Schedule interval using cron expression or preset value like @hour etc.,
                "catchup": False
                }
 
with DAG("BQ_End_to_end_Pipeline_gcs_to_SP",
         default_args=default_args,
         description="Run the End to end data pipeline using BQ SP",
         template_searchpath="/home/airflow/gcs/dags/script"
        ) as dag:     
 
        gcs_object_exists = GCSObjectExistenceSensor(
        task_id="gcs_object_exists_task",
        bucket='incpetez-data-samples',
        object='dataset/bqdata/ext_src_data/custs_header_20230908',
        timeout=60,
        mode='poke'
        )

       
        call_stored_procedure = BigQueryInsertJobOperator(
        task_id="call_stored_procedure",
        configuration={
        "query": {
        "query": "CALL `curatedds.sp_cust_etl`('gs://incpetez-data-samples/dataset/bqdata/ext_src_data/custs_header_20230908','incr'); ","useLegacySql": False,}
        },
        location=location,
        )

        gcs_object_exists>>call_stored_procedure