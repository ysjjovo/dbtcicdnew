
from pathlib import Path

from airflow import DAG
from pendulum import datetime
from cosmos.config import ProjectConfig

from cosmos import (
    ProfileConfig,
    ExecutionConfig,
    ExecutionMode,
    DbtTaskGroup,
)
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from airflow.operators.empty import EmptyOperator

PROJECT_DIR = Path("dags/dbt/dbtcicdnew/")


with DAG(
    dag_id="dbtcicdnew",
    start_date=datetime(2022, 11, 27),
    doc_md=__doc__,
    catchup=False,
) as dag:


    run_models = DbtTaskGroup(
        profile_config=ProfileConfig(
            profile_name="dbtcicdnew",
            target_name="dev",
            profile_mapping=RedshiftUserPasswordProfileMapping(
                conn_id="redshift_default",
                profile_args={
                    "schema": "public",
                },
            ),
        ),
        project_config=ProjectConfig(PROJECT_DIR),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.KUBERNETES,
        ),
        operator_args={
            "do_xcom_push": False,
            "project_dir":"/app",
            "image": "139260835254.dkr.ecr.us-east-2.amazonaws.com/dbtcicdnew:1.0",
            "get_logs": True,
            "is_delete_operator_pod": False,
            "namespace": "mwaa",
            "config_file": "/usr/local/airflow/dags/kubeconfig",
            "in_cluster": False,
            "image_pull_policy": "Always",
        },
    )
    e1 = EmptyOperator(task_id="pre_dbt")
    e2 = EmptyOperator(task_id="post_dbt")
    e1 >> run_models >> e2
