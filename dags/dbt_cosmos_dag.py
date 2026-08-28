"""
Airflow 3 + Astronomer Cosmos DAG for the dbt_demo sandbox.

Architecture:
1. Task `init_elementary_tables` (DbtRunLocalOperator): Runs
   `dbt run --select package:elementary` once, using the same Cosmos
   ProfileConfig / postgres_default mapping as the rest of the DAG
   (host=postgres inside Docker). Keeps ~35 Elementary models out of the UI.
2. Group `dbt_project` (DbtTaskGroup): Renders only project seeds, staging,
   marts, and tests; RenderConfig excludes `package:elementary`.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

from cosmos import (
    DbtRunLocalOperator,
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", AIRFLOW_HOME / "dbt_demo"))
DBT_EXECUTABLE = os.environ.get(
    "DBT_EXECUTABLE",
    str(AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt"),
)

# Target configuration for dbt PostgreSQL schema mapping
profile_config = ProfileConfig(
    profile_name="demo_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={
            "schema": "dbt_marts",
            "threads": 4,
        },
    ),
)

# ProjectConfig with install_dbt_deps=True forces Cosmos to pull dependencies prior to parsing
project_config = ProjectConfig(
    dbt_project_path=str(DBT_PROJECT_DIR),
    project_name="dbt_demo",
    install_dbt_deps=True,
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE,
    execution_mode=ExecutionMode.LOCAL,
)

# RenderConfig excluding internal Elementary package models from expanding into tasks
render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    test_behavior=TestBehavior.BUILD,
    emit_datasets=False,
    exclude=["package:elementary"],
)

with DAG(
    dag_id="dbt_cosmos_demo",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "cosmos", "elementary", "sandbox"],
    default_args={
        "owner": "data-engineering",
        "retries": 0,
    },
    max_active_runs=1,
) as dag:

    # 1. Create/update Elementary metadata tables via Cosmos profile (postgres_default → host postgres)
    init_elementary_tables = DbtRunLocalOperator(
        task_id="init_elementary_tables",
        project_dir=str(DBT_PROJECT_DIR),
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE,
        dbt_cmd_flags=["--select", "package:elementary"],
        install_deps=True,
        append_env=True,
    )

    # 2. Clean Cosmos graph containing only project seeds, models, and tests
    dbt_models_group = DbtTaskGroup(
        group_id="dbt_project",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={
            "append_env": True,
        },
    )

    # Elementary schema initialization runs prior to project task execution
    init_elementary_tables >> dbt_models_group
