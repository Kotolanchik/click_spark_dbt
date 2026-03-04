## dbt — архитектура, особенности, примеры

### Что это и зачем

**dbt (data build tool)** — инструмент для управления трансформациями в аналитическом хранилище через:

- **SQL-модели** (select-запросы как исходный код),
- **DAG зависимостей** (порядок сборки),
- **материализации** (view/table/incremental),
- **тесты и документацию** (качество и прозрачность),
- **макросы** (повторное использование логики через Jinja).

dbt отлично подходит для слоя «преобразования и витрин» поверх DWH/OLAP (в т.ч. ClickHouse).

---

### Архитектура (как работает внутри)

Упрощённый жизненный цикл:

1) **Parse**: dbt читает `dbt_project.yml`, `models/**/*.sql`, `*.yml`, макросы, строит граф зависимостей.
2) **Compile**: SQL шаблоны (Jinja) превращаются в «чистый SQL», разворачиваются `ref()`/`source()`.
3) **Run**: dbt исполняет узлы графа в правильном порядке в целевой БД через **adapter**.
4) **Test**: выполняются тесты (generic + singular).
5) **Docs**: генерируется каталог объектов, lineage, описания.

Ключевые сущности в проекте:

- **Model**: SQL-файл, который возвращает `SELECT ...` (без `CREATE TABLE` руками).
- **Source**: описание «сырых» таблиц/стримов, которые создаёт не dbt.
- **Seed**: CSV → таблица (удобно для справочников/маппингов).
- **Snapshot**: фиксация истории изменений (SCD) *по правилам*.
- **Macro**: Jinja-функции/материализации/утилиты.
- **Exposure**: потребители данных (дашборды/ML/приложения) для прозрачности ownership.

---

### Особенности и практики

- **DAG как контракт**: зависимости задаются через `ref('model')` и `source('src','table')`. Это лучше, чем «ручной порядок».
- **Окружения**: `dev`/`prod` — разные схемы/кластеры/параметры.
- **Инкрементальные модели**:
  - dbt строит только «новую часть» данных вместо полной пересборки;
  - в ClickHouse часто выбирают стратегии *append-only* (или `delete+insert` по партициям/ключам, если нужно).
- **Тесты**:
  - базовые: `not_null`, `unique`, `accepted_values`, `relationships`;
  - важнее всего — тестировать ключевые бизнес-инварианты, а не всё подряд.
- **Документация**: описания в `schema.yml` + `dbt docs generate` дают lineage и контекст «что это за таблица».

---

### ClickHouse + dbt: что важно знать

dbt работает через **адаптер**. Для ClickHouse часто используют `dbt-clickhouse` (Core).

Практические моменты:

- **Транзакционность/DDL**: некоторые операции выполняются как последовательность DDL/DML, и поведение зависит от движков таблиц.
- **Incremental**: «обновления» в ClickHouse обычно делаются не `UPDATE`, а:
  - запись новой версии в `ReplacingMergeTree(version)` и чтение «последней версии»,
  - или пересборка партиций, или `delete+insert` по диапазону.
- **Сортировочный ключ**: если dbt создаёт физические таблицы, важно задавать `ORDER BY`/`PARTITION BY` через конфиг (adapter-specific).

---

### Минимальный пример проекта (структура)

В этом репозитории лежит простой пример в `dbt/`:

- `dbt/dbt_project.yml`
- `dbt/models/` (staging + marts)
- `dbt/models/schema.yml` (описания + тесты)

Сборка делает:

1) staging (`stg_events`) — типизация/нормализация «сырых» событий из `analytics.events`
2) витрина (`fct_daily_revenue`) — дневная выручка по платформам

---

### Настройка подключения (`profiles.yml`)

`profiles.yml` хранит креды и обычно лежит в `~/.dbt/profiles.yml` (не в репозитории).

Шаблон профиля для этого репо:

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

---

### Команды dbt (часто используемые)

```bash
# Проверка, что подключение ок
dbt debug --project-dir dbt --profiles-dir ~/.dbt

# Собрать модели
dbt run --project-dir dbt --profiles-dir ~/.dbt

# Запустить тесты
dbt test --project-dir dbt --profiles-dir ~/.dbt

# Сгенерировать документацию
dbt docs generate --project-dir dbt --profiles-dir ~/.dbt
dbt docs serve --project-dir dbt --profiles-dir ~/.dbt
```

Выборка по подграфу/тегам:

```bash
dbt run --project-dir dbt --profiles-dir ~/.dbt --select +fct_daily_revenue
```

