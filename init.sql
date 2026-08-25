-- Initialize databases and schemas for the dbt + Elementary + Airflow sandbox.
-- Runs against POSTGRES_DB=demo_db on first container start.

CREATE DATABASE airflow;

CREATE SCHEMA IF NOT EXISTS dbt_test;
CREATE SCHEMA IF NOT EXISTS dbt_marts;
CREATE SCHEMA IF NOT EXISTS elementary;
CREATE SCHEMA IF NOT EXISTS dbt_test_failures;

GRANT ALL ON SCHEMA dbt_test TO postgres;
GRANT ALL ON SCHEMA dbt_marts TO postgres;
GRANT ALL ON SCHEMA elementary TO postgres;
GRANT ALL ON SCHEMA dbt_test_failures TO postgres;
