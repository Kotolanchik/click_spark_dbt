## Сквозной пример: Spark → ClickHouse → dbt

Цель: получить простой, но реалистичный пайплайн:

- **Spark** генерирует «сырые события» и пишет их в **Parquet** (как в Data Lake),
- **ClickHouse** загружает Parquet в raw-таблицу,
- **dbt** строит staging и витрину (дневная выручка по платформам) + тесты.

### Предпосылки

- Docker + Docker Compose
- Spark 3.x (локально) или любой способ запустить `spark-submit`
- Python (если запускаете PySpark локально)
- dbt Core + адаптер для ClickHouse (например, `dbt-clickhouse`)

---

### 1) Поднять ClickHouse

```bash
docker compose up -d
```

Проверка:

```bash
curl -s http://localhost:8123/ping
```

---

### 2) Создать БД и raw-таблицу

```bash
docker exec -i clickhouse clickhouse-client < examples/clickhouse_init.sql
```

---

### 3) Сгенерировать Parquet в Spark

Скрипт лежит в `spark_jobs/generate_events_parquet.py` и пишет Parquet в `examples/events.parquet/`.

```bash
spark-submit spark_jobs/generate_events_parquet.py --n 200000 --out examples/events.parquet
```

Проверка, что Parquet появился:

```bash
ls -la examples/events.parquet
```

---

### 4) Загрузить Parquet в ClickHouse

`docker-compose.yml` монтирует папку `./examples` в контейнер как `/examples`, поэтому ClickHouse видит Parquet-файлы.

```bash
docker exec -i clickhouse clickhouse-client --multiquery --query "
  INSERT INTO analytics.events (event_time, user_id, session_id, event_name, platform, price, props_json)
  SELECT
    parseDateTime64BestEffort(event_time, 3)       AS event_time,
    toUInt64(user_id)                              AS user_id,
    toUUID(session_id)                             AS session_id,
    event_name,
    platform,
    toDecimal64(price, 2)                          AS price,
    props_json
  FROM file('/examples/events.parquet', Parquet);
"
```

Быстрая проверка:

```bash
docker exec -i clickhouse clickhouse-client --query "
  SELECT event_name, count() AS c
  FROM analytics.events
  GROUP BY event_name
  ORDER BY c DESC;
"
```

---

### 5) Собрать витрины dbt поверх ClickHouse

1) Создайте `profiles.yml` (см. шаблон в `docs/dbt.md`) в `~/.dbt/profiles.yml`.
2) Запустите:

```bash
dbt debug --project-dir dbt --profiles-dir ~/.dbt
dbt run   --project-dir dbt --profiles-dir ~/.dbt
dbt test  --project-dir dbt --profiles-dir ~/.dbt
```

Проверка результата в ClickHouse:

```bash
docker exec -i clickhouse clickhouse-client --query "
  SELECT *
  FROM analytics.fct_daily_revenue
  ORDER BY event_date DESC, platform
  LIMIT 20;
"
```

