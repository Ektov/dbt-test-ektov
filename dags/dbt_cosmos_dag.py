"""
Airflow 3 + Astronomer Cosmos DAG for the dbt_demo sandbox.

Runs: dbt seed → dbt run → dbt test (incl. unit tests + Elementary anomalies).

Notes on intentionally failing tests:
-----------------------------------
Generic tests on stg_users / stg_orders are designed to FAIL (duplicate user_id,
null email, invalid order_status). They use severity: warn + store_failures: true,
so invalid rows are persisted in dbt_test_failures while the Cosmos test task
can still succeed (Elementary anomaly tests also use severity: warn).
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from cosmos import (
    DbtDag,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Determine Airflow home and dbt project directories dynamically
AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", AIRFLOW_HOME / "dbt_demo"))
DBT_EXECUTABLE = os.environ.get(
    "DBT_EXECUTABLE",
    str(AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt"),
)

# Profile configuration connecting Airflow's postgres_default connection to dbt
profile_config = ProfileConfig(
    profile_name="demo_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={
            # Target schema for models in PostgreSQL
            "schema": "dbt_marts",
            "threads": 4,
        },
    ),
)

# ProjectConfig with install_dbt_deps=True forces Cosmos to run `dbt deps` BEFORE `dbt ls` parsing
project_config = ProjectConfig(
    dbt_project_path=str(DBT_PROJECT_DIR),
    project_name="dbt_demo",
    install_dbt_deps=True,  # Automatically downloads dbt packages before parsing the project graph
)

# Execution mode using local dbt binary execution inside the Airflow environment
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE,
    execution_mode=ExecutionMode.LOCAL,
)

# RenderConfig filtering out internal Elementary package models while preserving seeds and custom models
render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    test_behavior=TestBehavior.AFTER_ALL,
    emit_datasets=False,
    # Our models/seeds + Elementary package models (needed before Elementary tests).
    select=["path:models", "path:seeds", "package:elementary"],
)

# Main Airflow 3 DAG definition
dbt_cosmos_dag = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    operator_args={
        "append_env": True,
    },
    # Airflow 3 uses `schedule` parameter for cron/interval settings
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="dbt_cosmos_demo",
    tags=["dbt", "cosmos", "elementary", "sandbox"],
    default_args={
        "owner": "data-engineering",
        "retries": 0,
    },
    max_active_runs=1,
)