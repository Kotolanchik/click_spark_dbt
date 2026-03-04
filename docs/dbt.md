## dbt — полное описание возможностей (коротко и по делу)

### Определение

**dbt (data build tool)** — инструмент, который превращает SQL‑запросы в **управляемый DAG** трансформаций внутри вашей БД/движка: сборка, инкрементальность, тесты, документация, переиспользование, окружения, CI.

---

### Как работает (модель исполнения)

- **Parse**: читает `dbt_project.yml`, `models/**/*.sql`, `*.yml`, макросы/пакеты → строит граф.
- **Compile**: разворачивает Jinja (`ref()`, `source()`, `var()`, `config()`) → «чистый SQL».
- **Execute**: выполняет модели/тесты/снапшоты через **adapter** (конкретная БД/движок).
- **Artifacts**: пишет артефакты сборки (для lineage/CI/автоматизации).

---

### Что dbt умеет (по категориям)

#### 1) Управление моделями данных

- **Models**: SQL‑модели (обычно разделяют на `staging` → `intermediate` → `marts`).
- **Dependencies**: `ref('model')`, `source('src','table')` + «графовые» селекторы.
- **Materializations**:
  - `view`, `table`
  - `incremental` (пересборка только новой части)
  - `ephemeral` (CTE без создания объекта в БД)
  - **кастомные материализации** через макросы (adapter‑specific)
- **Полная пересборка**: `--full-refresh` (важно для `incremental`).
- **Pre/Post hooks**: DDL/DML до/после модели (например, настройки сессии, оптимизации, grants).

#### 2) Макросы и переиспользование (Jinja)

- **Macros**: функции/шаблоны SQL, общие выражения, кастомная логика.
- **Dispatch**: «полиморфизм» под разные адаптеры (например, разные функции дат).
- **Vars/Env**: `var()`, `env_var()` для параметризации.
- **Operations**: `dbt run-operation my_macro --args '{...}'` для админ‑операций/утилит.

#### 3) Тестирование данных (Data Quality)

- **Generic tests (schema tests)**: `not_null`, `unique`, `accepted_values`, `relationships` + кастомные.
- **Singular tests**: SQL‑запрос, который должен вернуть **0 строк** (нарушения).
- **Severity/Store failures** (зависит от версии/адаптера): «ошибка/предупреждение», запись нарушений в таблицы.
- **Source freshness**: проверки «насколько свежие» данные в источниках.

#### 4) Документация и lineage

- **Descriptions** в `schema.yml`, `docs`‑блоки для длинных описаний.
- **Lineage** (граф зависимостей) и каталог объектов.
- Команды: `dbt docs generate`, `dbt docs serve`.

#### 5) Управление источниками и контрактами

- **Sources**: единый реестр «сырых» таблиц, ownership, freshness.
- **Contracts** (в некоторых режимах): контроль схемы/типов (фича зависит от версии и адаптера).

#### 6) Seeds / Snapshots / Exposures

- **Seeds**: CSV → таблица (справочники, маппинги).
- **Snapshots**: SCD‑история изменений (стратегии `timestamp`/`check`).
- **Exposures**: фиксация потребителей (дашборды/ML/приложения) для ответственности и impact‑анализа.

#### 7) Пакеты и переиспользование экосистемы

- **Packages**: `packages.yml` + `dbt deps` (подключение готовых макросов/моделей).
- Версионирование зависимостей и совместимость.

#### 8) Окружения, окружная изоляция, CI/CD

- **Profiles/targets**: разные подключения `dev/stage/prod`.
- **Селекторы**: запуск подмножества графа (по путям/тегам/ресурсам).
- **State‑based selection**: запуск только изменившегося (`state:modified`, `--state path/to/artifacts`).
- **Defer**: в dev можно «ссылаться» на прод‑артефакты, чтобы не пересобирать всё.
- Подходит для CI: `dbt build` (run + test + snapshots) на PR.

#### 9) Артефакты (важно для автоматизации)

dbt генерирует файлы вроде:

- `manifest.json` (полный граф, конфиги, lineage)
- `run_results.json` (результаты выполнения)
- `catalog.json` (колонки/типы/статистика для docs)

Они используются для:

- диффов в CI,
- выбора подграфов,
- построения lineage в сторонних инструментах.

---

### Что dbt не делает (важно понимать границы)

- Не ingests данные (это делают Spark/Flink/Airbyte/Kafka/…).
- Не является оркестратором (обычно dbt запускают Airflow/Dagster/Argo/CI).
- Не заменяет управление доступами/кластером/стоимостью запросов (это слой DWH/infra).

---

### ClickHouse + dbt (коротко, но критично)

- dbt работает через **адаптер** (часто `dbt-clickhouse`).
- **Incremental в ClickHouse** почти всегда строят как *append‑only* или *пересборка диапазона/партиции* (а не `UPDATE`).
- Для физических таблиц важно задавать ClickHouse‑специфику (движок/`ORDER BY`/`PARTITION BY`) через конфиг/макросы адаптера.

---

### Минимальный пример в этом репозитории

`dbt/`:

- `models/staging/stg_events.sql` — staging view
- `models/marts/fct_daily_revenue.sql` — витрина
- `models/schema.yml` — источники + тесты + описания

---

### Подключение (`profiles.yml`) и команды

`profiles.yml` обычно лежит в `~/.dbt/profiles.yml` (не коммитится).

```yaml
clickhouse_dbt_spark:
  target: dev
  outputs:
    dev:
      type: clickhouse
      host: localhost
      port: 8123
      user: default
      password: ""
      database: analytics
      schema: analytics
      secure: false
      verify: false
```

Команды:

```bash
dbt debug --project-dir dbt --profiles-dir ~/.dbt
dbt build --project-dir dbt --profiles-dir ~/.dbt
dbt docs generate --project-dir dbt --profiles-dir ~/.dbt
```

Селекторы (примеры):

```bash
# Модель + все апстрим зависимости
dbt run --project-dir dbt --profiles-dir ~/.dbt --select +fct_daily_revenue

# По тегу
dbt run --project-dir dbt --profiles-dir ~/.dbt --select tag:nightly
```

