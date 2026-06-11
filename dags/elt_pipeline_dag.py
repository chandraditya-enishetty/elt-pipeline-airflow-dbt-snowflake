"""
dags/elt_pipeline_dag.py
========================
Airflow DAG that orchestrates the full ELT pipeline using Astronomer Cosmos.
Cosmos converts each dbt model into a separate Airflow task automatically,
so you get full task-level visibility, retries, and lineage in the Airflow UI.

Schedule: daily at 6am UTC
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import LoadMode

# ── paths ─────────────────────────────────────────────────────────────────────
DBT_PROJECT_PATH = Path("/opt/airflow/dbt/elt_pipeline")
DBT_EXECUTABLE   = Path("/opt/airflow/dbt_venv/bin/dbt")

# ── Snowflake connection profile ───────────────────────────────────────────────
profile_config = ProfileConfig(
    profile_name="elt_pipeline",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",          # configured in Airflow UI → Admin → Connections
        profile_args={
            "database": os.environ.get("SNOWFLAKE_DATABASE", "elt_db"),
            "schema":   "staging",
        },
    ),
)

# ── Default DAG args ───────────────────────────────────────────────────────────
default_args = {
    "owner":            "chandraditya",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ── DAG definition ─────────────────────────────────────────────────────────────
elt_pipeline_dag = DbtDag(
    # DAG metadata
    dag_id="elt_pipeline",
    schedule_interval="0 6 * * *",        # daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["elt", "dbt", "snowflake", "tpch"],
    doc_md="""
    ## ELT Pipeline — Airflow + dbt + Snowflake

    **Source:** Snowflake TPC-H sample data (`snowflake_sample_data.tpch_sf1`)

    **Layers:**
    - **Staging** → views: `stg_tpch_orders`, `stg_tpch_lineitems`, `stg_tpch_customers`
    - **Intermediate** → views: `int_order_items`
    - **Marts** → tables: `fct_orders`, `dim_customers`

    **Tests:** dbt tests run automatically after each model group.

    **Orchestrated by:** Astronomer Cosmos (each dbt model = one Airflow task)
    """,

    # dbt project config
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),

    # Profile config
    profile_config=profile_config,

    # Execution config
    execution_config=ExecutionConfig(
        dbt_executable_path=str(DBT_EXECUTABLE),
    ),

    # Render config — runs tests after each node
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        test_behavior="after_each",         # run dbt tests after every model
    ),

    operator_args={
        "install_deps": True,               # installs dbt packages on first run
        "full_refresh": False,              # set True to rebuild all tables from scratch
    },
)
