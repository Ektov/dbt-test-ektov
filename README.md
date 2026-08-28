# Sandbox dbt + PostgreSQL + Airflow 3 (Cosmos) + Elementary Data

Локальный стенд для проверки того, как **dbt** и **Elementary** обрабатывают все типы тестов (Generic, Native Unit Tests dbt 1.8+, Elementary Anomalies) и как сохраняются проваленные тесты и невалидные строки (`store_failures`).

## Состав

| Компонент | Назначение |
|-----------|------------|
| PostgreSQL 16 | Хранилище (`demo_db`) + метаданные Airflow (`airflow`) |
| dbt-postgres ≥1.8 | Модели, seeds, generic/unit tests |
| Elementary Data | Аномалии + таблицы результатов тестов |
| Airflow 3 + Astronomer Cosmos | Оркестрация `seed → run → test` |

## Структура проекта

```
.
├── docker-compose.yml      # postgres + airflow (api/scheduler/dag-processor)
├── Dockerfile.airflow      # Airflow 3 + cosmos (no dbt extra) + /opt/airflow/dbt_venv
├── init.sql                # schemas + CREATE DATABASE airflow
├── profiles.yml            # dbt profile (localhost)
├── requirements.txt
├── dags/
│   └── dbt_cosmos_dag.py   # Cosmos DbtDag
├── dbt_demo/               # dbt-проект
│   ├── dbt_project.yml     # store_failures: true
│   ├── packages.yml        # elementary-data/elementary
│   ├── seeds/              # CSV с намеренными ошибками
│   └── models/
│       ├── staging/        # stg_* + generic tests
│       └── marts/          # fct_orders, dim_customers, unit + elementary tests
└── scripts/generate_seeds.py
```

## Быстрый старт (Docker)

```bash
# 1. AIRFLOW_UID — используйте 50000 (пользователь образа apache/airflow).
#    На macOS НЕ ставьте $(id -u): UID 501 нет в /etc/passwd контейнера.
cp .env.example .env
# На Linux при необходимости: echo "AIRFLOW_UID=$(id -u)" > .env

# 2. Собрать и поднять
docker compose build
docker compose up -d

# 3. Дождаться healthcheck Postgres и init Airflow
docker compose ps
docker compose logs airflow-init
```

- **UI Airflow**: http://localhost:8080 — логин `admin` / `admin`
  - В Airflow 3 `SIMPLE_AUTH_MANAGER_USERS=admin:admin` означает **username:role**, не пароль.
  - Фиксированный пароль задан в [`config/simple_auth_passwords.json`](config/simple_auth_passwords.json).
  - Образ: **Airflow 3.1.8** (Cosmos 1.15 AF3 plugin требует ≥ 3.1).
- **PostgreSQL**: `localhost:5432`, БД `demo_db`, user/pass `postgres` / `postgres`

### Подключение из PyCharm / DBeaver

| Параметр | Значение |
|----------|----------|
| Host | `localhost` |
| Port | `5432` |
| Database | `demo_db` |
| User | `postgres` |
| Password | `postgres` |
| Schemas | `dbt_test`, `dbt_marts`, `elementary`, `dbt_test_failures` |

## Локальный запуск dbt (без Airflow)

Требуется запущенный Postgres (`docker compose up -d postgres`).

> **Python:** для локального `dbt` используйте **3.11–3.12**. Python 3.14 пока плохо совместим с dbt/mashumaro. В Docker используется 3.12.

```bash
# venv (опционально)
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Профиль: либо скопировать, либо указать каталог
cp profiles.yml ~/.dbt/profiles.yml
# или:
export DBT_PROFILES_DIR="$(pwd)"

cd dbt_demo
dbt deps          # скачает elementary
dbt seed          # загрузит CSV → schema dbt_test
dbt run           # staging + marts + elementary models (on-run-end)
dbt test          # generic + unit + elementary tests
dbt test --select test_type:unit   # только unit-тесты (должны PASS)
```

### Elementary отчёт

После `dbt run` / `dbt test` (чтобы заполнились таблицы Elementary):

```bash
# из корня репозитория, с тем же profiles.yml
edr report --project-dir dbt_demo --profiles-dir . --profile-target dev
# HTML-отчёт появится в edr_target/ (или путь, который выведет CLI)
```

Либо внутри контейнера Airflow (нужен профиль с `host: postgres` — сгенерируйте временный или правьте target; смонтированный `profiles.yml` с `localhost` из контейнера к Postgres не подключится):

```bash
docker compose exec airflow-scheduler bash -c \
  'cd /opt/airflow/dbt_demo && /opt/airflow/dbt_venv/bin/edr report --profiles-dir . --profile-target dev'
```

Предпочтительнее запускать `edr` **с хоста** (`--profiles-dir .` у корня репо, Postgres на `localhost:5432`).
## Намеренные ошибки в seeds

| Файл | Ошибка | Какой тест падает |
|------|--------|-------------------|
| `raw_users.csv` | дубликат `user_id=1` | `unique` на `stg_users.user_id` |
| `raw_users.csv` | пустой `email` у `user_id=9999` | `not_null` / `unique` на `email` |
| `raw_orders.csv` | `order_status=invalid_unknown_status` | `accepted_values` на `stg_orders.order_status` |

Перегенерация CSV:

```bash
python3 scripts/generate_seeds.py
```

## Типы тестов в проекте

1. **Generic** — `models/staging/schema.yml` (`unique`, `not_null`, `accepted_values`, `relationships`). Намеренно падающие тесты имеют `severity: warn` и `store_failures: true`.
2. **Native Unit Tests (dbt 1.8+)** — `models/marts/fct_orders.yml` → `test_fct_orders_calculated_total` (**должен PASS**).
3. **Elementary Anomalies** — `models/marts/schema.yml`:
   - `elementary.volume_anomalies` на `fct_orders` (`order_date`, day bucket)
   - `elementary.dimension_anomalies` по `category` на `fct_orders`

> **Caveat:** аномалии Elementary обычно требуют историю запусков. На первом `dbt test` они могут пропускаться / давать warn / не иметь baseline — это нормально для sandbox. Повторите `dbt run && dbt test` несколько дней (или сдвиньте даты в seeds) для более реалистичного сигнала.

## Airflow DAG (Cosmos)

DAG id: **`dbt_cosmos_demo`**

Порядок задач:

1. **`init_elementary_tables`** — один `DbtRunLocalOperator` с `--select package:elementary` (создаёт таблицы Elementary без ~35 отдельных тасков в UI).
2. **`dbt_project`** (`DbtTaskGroup`) — seeds / staging / marts / tests; `RenderConfig.exclude=["package:elementary"]`, чтобы пакет не дублировался в графе.

Конфиг:

- `ProjectConfig` → `/opt/airflow/dbt_demo`
- `ExecutionConfig.dbt_executable_path` → `/opt/airflow/dbt_venv/bin/dbt` (отдельный venv; Cosmos без `[dbt-postgres]` extra)
- `PostgresUserPasswordProfileMapping(conn_id="postgres_default")` — **host=`postgres`** (имя сервиса Docker), не `localhost`
- `TestBehavior.AFTER_ALL` — сначала модели, потом тесты
- `install_deps: True` — `dbt deps` перед командами

> **profiles.yml:** корневой файл с `host: localhost` — для dbt/edr **на хосте**. В контейнерах Cosmos **не** читает его для run/test: профиль строится из Airflow Connection `postgres_default`. Не запускайте сырой `dbt --profiles-dir /opt/airflow/dbt_demo` внутри контейнера — compose монтирует `profiles.yml` с `localhost`, и подключение к Postgres упадёт с `Connection refused`.

Откройте UI → DAG `dbt_cosmos_demo` → Trigger.

Падающие generic-тесты с `severity: warn` не валят весь DAG, но строки всё равно пишутся в `store_failures`. Если поставить `severity: error`, task теста будет красным — это ожидаемо.

## SQL: просмотр failed tests и невалидных строк

Подключайтесь к `demo_db`.

### 1. Результаты тестов Elementary (не pass)

```sql
-- Статусы failed / warn / error (названия статусов могут отличаться по версии)
SELECT
    id,
    test_unique_id,
    model_unique_id,
    detection_time,
    status,
    test_name,
    test_short_name,
    failures,
    failed_row_count,
    test_results_description
FROM elementary.elementary_test_results
WHERE lower(status) NOT IN ('pass', 'passed')
ORDER BY detection_time DESC
LIMIT 100;
```

```sql
-- Сводка по статусам
SELECT status, count(*) AS cnt
FROM elementary.elementary_test_results
GROUP BY 1
ORDER BY 2 DESC;
```

### 2. Таблицы store_failures (невалидные строки)

При `+store_failures: true` и `+schema: dbt_test_failures` dbt создаёт таблицы с failed rows в схеме `dbt_test_failures` (имена обычно вида `<test_name>` / hash).

```sql
-- Найти все таблицы с failed rows
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('dbt_test_failures', 'dbt_marts', 'dbt_test', 'elementary')
  AND (
        table_name ILIKE '%not_null%'
     OR table_name ILIKE '%unique%'
     OR table_name ILIKE '%accepted_values%'
     OR table_name ILIKE '%fail%'
     OR table_name ILIKE 'dbt_test__audit%'
  )
ORDER BY table_schema, table_name;
```

```sql
-- Универсальный просмотр: все user-таблицы в схеме failures
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'dbt_test_failures'
ORDER BY table_name;
```

Пример (имя таблицы зависит от версии dbt / имени теста — подставьте из запроса выше):

```sql
-- Пример: failed rows для not_null email / unique user_id
-- SELECT * FROM dbt_test_failures.<имя_таблицы_из_information_schema> LIMIT 50;
```

Быстрый поиск содержимого по всем failure-таблицам через `psql`:

```bash
docker compose exec postgres \
  psql -U postgres -d demo_db -c "\dt dbt_test_failures.*"
```

### 3. Проверка исходных «битых» данных

```sql
-- Дубликаты user_id
SELECT user_id, count(*) AS cnt
FROM dbt_test.raw_users
GROUP BY 1
HAVING count(*) > 1;

-- Пустые / NULL email в staging
SELECT *
FROM dbt_test.stg_users
WHERE email IS NULL OR email = '';

-- Невалидный статус заказа
SELECT *
FROM dbt_test.stg_orders
WHERE order_status = 'invalid_unknown_status';
```

### 4. Marts после успешного run

```sql
SELECT * FROM dbt_marts.fct_orders ORDER BY order_id, item_id LIMIT 20;
SELECT * FROM dbt_marts.dim_customers ORDER BY user_id LIMIT 20;
```

### 5. dbt run_results / Elementary model runs (если есть)

```sql
SELECT *
FROM elementary.dbt_run_results
ORDER BY generated_at DESC
LIMIT 50;
```

```sql
SELECT *
FROM elementary.dbt_tests
ORDER BY name
LIMIT 50;
```

## Важные настройки

- **`store_failures: true`** — в `dbt_project.yml` (`tests.dbt_demo`) и на отдельных тестах. Невалидные строки сохраняются в `dbt_test_failures`.
- **`macros/generate_schema_name.sql`** — кастомные схемы (`dbt_test`, `dbt_marts`, `elementary`) используются **как есть**, без префикса target-схемы.
- **`on-run-end: elementary.on_run_end()`** — пишет артефакты Elementary после прогона.

## Типичные проблемы

| Симптом | Что сделать |
|---------|-------------|
| Port 5432 занят | Остановить локальный Postgres или сменить host-порт в compose |
| `docker compose build` / `ResolutionTooDeep` | Не ставить `astronomer-cosmos[dbt-postgres]` в образ Airflow; dbt только в `dbt_venv`. Cosmos pin + `--constraint` из `constraints-3.1.8` |
| `ImportError: context_to_airflow_vars` | Нужен **astronomer-cosmos ≥ 1.10** (Airflow 3). В образе — `1.15.1` без `[dbt-postgres]` |
| `ModuleNotFoundError: No module named 'cosmos'` | Не добавляйте `dbt_venv` в начало `PATH` — иначе `python` берётся из venv без cosmos. Используйте `DBT_EXECUTABLE` |
| `Cosmos AF3 plugin requires Airflow >= 3.1` | Базовый образ должен быть **≥ 3.1** (сейчас `3.1.8-python3.12`) |
| `POST /auth/token` → 401 при admin/admin | `admin:admin` в конфиге = роль, не пароль. Пароль — в `config/simple_auth_passwords.json` или в логе api-server (`Password for user 'admin': ...`) |
| Все таски `failed` + `state mismatch` / `Connection refused` | В Airflow 3 задайте `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/` (не localhost) |
| `fct_orders.test` / on-run-end: `relation elementary.test_*__metrics__tmp_* does not exist` | Не дублируйте `on-run-end: elementary.on_run_end()` в `dbt_project.yml` — пакет Elementary уже регистрирует хук; второй вызов чистит temp-таблицы и падает. Также не ставьте `elementary: +materialized: table` |
| `init_elementary_tables` / dbt: `connection to server at "localhost" … refused` | В Docker Postgres — сервис `postgres`, не `localhost`. Используйте Cosmos `ProfileMapping` / `postgres_default`; не полагайтесь на смонтированный `profiles.yml` с `host: localhost` |
| `getpwuid(): uid not found: 501` / no username | В `.env` задать `AIRFLOW_UID=50000` (не `$(id -u)` на macOS), затем `docker compose up -d --force-recreate` |
| `dbt deps` fail | Нужен интернет; повторить из `dbt_demo/` |
| Anomalies «не о чём судить» | Нужна история; повторить прогоны |
| Airflow DAG не виден | `docker compose logs airflow-dag-processor`; проверить синтаксис DAG |
| Permission denied на volume | Оставить `AIRFLOW_UID=50000`; при необходимости `chmod -R a+rwX dags dbt_demo` (logs — named volume) |
| Нужен чистый Postgres | `docker compose down -v` (удалит volume!) и снова `up` |

## Остановка

```bash
docker compose down          # контейнеры
docker compose down -v       # + удалить данные Postgres
```
