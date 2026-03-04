# ClickHouse + dbt + Spark — документация и живые примеры

Этот репозиторий — **конспект-справочник** по трём технологиям и их совместному использованию:

- **ClickHouse**: колоночная аналитическая СУБД для быстрых агрегаций и BI
- **dbt**: слой трансформаций в DWH через SQL + DAG + тесты/документацию
- **Apache Spark**: распределённая обработка данных (batch/streaming), ETL/ELT

## Что внутри

- `docs/clickhouse.md` — архитектура ClickHouse, особенности, паттерны и примеры SQL
- `docs/dbt.md` — архитектура dbt, ключевые сущности, практики и примеры проекта
- `docs/spark.md` — архитектура Spark, особенности производительности, примеры PySpark
- `docs/ad-hoc.md` — определение ad‑hoc и варианты реализации
- `docs/examples-end-to-end.md` — сквозной пример: **Spark → ClickHouse → dbt**
- `docker-compose.yml` — локальный ClickHouse для запуска примеров
- `spark_jobs/` — пример Spark job (генерация событий и загрузка в ClickHouse)
- `dbt/` — минимальный пример dbt-проекта под ClickHouse

## Быстрый старт (локально)

1) Поднять ClickHouse:

```bash
docker compose up -d
```

2) Открыть UI ClickHouse (по желанию): `http://localhost:8123/play`

3) Прогнать сквозной пример — см. `docs/examples-end-to-end.md`.

## Примечание про профили dbt

Файл `profiles.yml` **не коммитится** (содержит подключения). Шаблон и команды есть в `docs/dbt.md`.

