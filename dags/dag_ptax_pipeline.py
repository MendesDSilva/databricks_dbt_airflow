
from datetime import datetime

from airflow import DAG

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior
from cosmos.config import RenderConfig


DBT_PROJECT_PATH = "/usr/local/airflow/db_dbt_airflow"
DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"


with DAG(
    dag_id="ptax_dbt_pipeline",
    description="Orquestracao dos modelos dbt no Databricks com Cosmos",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["databricks", "dbt", "cosmos", "ptax"],
) as dag:

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",

        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),

        profile_config=ProfileConfig(
            profile_name="db_dbt_airflow",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
        ),

        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            dbt_executable_path=DBT_EXECUTABLE,
        ),

        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
        ),
    )
