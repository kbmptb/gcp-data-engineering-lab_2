from datetime import datetime

from airflow import DAG
from airflow.providers.apache.beam.operators.beam import (
    BeamRunPythonPipelineOperator,
)

PROJECT_ID = "project-a0c99e7b-8c47-4075-b97"

with DAG(
    dag_id="Customers_Orders",
    start_date=datetime(2026, 9, 25),
    schedule="0 */12 * * *",
    catchup=False,
    tags=["dataflow"],
) as dag:

    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="run_dataflow_pipeline",
        py_file="gs://bucket280926-demo-dev/dataflow/Customers_Orders.py",
        runner="DataflowRunner",
        pipeline_options={
            "project": PROJECT_ID,
            "region": "europe-west1",
            "temp_location": "gs://bucket_temp_21092026/temp",
            "staging_location": "gs://bucket_temp_21092026/staging",
            "job_name": "customer-order-demo",
            "worker_machine_type": "e2-standard-2",
            "service_account_email":
"dataflow-sa@project-a0c99e7b-8c47-4075-b97.iam.gserviceaccount.com"
        }
    )