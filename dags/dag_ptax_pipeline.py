
from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)

from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
)

from cosmos.constants import (
    ExecutionMode,
    LoadMode,
    TestBehavior,
)

from cosmos.config import RenderConfig


# ============================================================
# 1. CONFIGURACOES DO PROJETO
# ============================================================

DBT_PROJECT_PATH = "/usr/local/airflow/db_dbt_airflow"

DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"

DBT_MANIFEST_PATH = (
    f"{DBT_PROJECT_PATH}/target/manifest.json"
)

DATABRICKS_JOB_ID = 479652614826982

DATABRICKS_CONN_ID = "databricks_default"


# ============================================================
# 2. DEFINICAO DA DAG
# ============================================================

with DAG(
    dag_id="ptax_dbt_pipeline",
    description=(
        "Pipeline PTAX: ingestao no Databricks, "
        "transformacao e testes dbt com Cosmos"
    ),
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "databricks",
        "dbt",
        "cosmos",
        "ptax",
        "delta-lake",
    ],
) as dag:

    # ========================================================
    # 3. INGESTAO PTAX - DATABRICKS
    # ========================================================

    ingestao_ptax = DatabricksRunNowOperator(
        task_id="ingestao_ptax",

        databricks_conn_id=DATABRICKS_CONN_ID,

        job_id=DATABRICKS_JOB_ID,

        wait_for_termination=True,

        polling_period_seconds=30,

        retries=2,

        retry_delay=timedelta(minutes=2),
    )

    # ========================================================
    # 4. TRANSFORMACAO DBT - COSMOS
    # ========================================================

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",

        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=DBT_MANIFEST_PATH,
        ),

        profile_config=ProfileConfig(
            profile_name="db_dbt_airflow",
            target_name="dev",
            profiles_yml_filepath=(
                f"{DBT_PROJECT_PATH}/profiles.yml"
            ),
        ),

        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            dbt_executable_path=DBT_EXECUTABLE,
        ),

        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_EACH,
        ),
    )

    # ========================================================
    # 5. ORQUESTRACAO
    # ========================================================

    ingestao_ptax >> dbt_models
