## Ad‑hoc: определение и реализации (ClickHouse / Spark / dbt)

### Определение

**Ad‑hoc** (в аналитике/данных) — это **разовые, интерактивные, исследовательские** запросы или вычисления, которые:

- запускаются «здесь и сейчас» под конкретный вопрос,
- не обязательно становятся частью регулярного пайплайна,
- часто меняются, итеративны и не требуют строгой продуктовой упаковки.

Важно отличать:

- **Ad‑hoc query**: «дай ответ на вопрос» (интерактивный SQL).
- **Ad‑hoc job**: «один раз посчитать/прогнать обработку» (Spark job / notebook).

---

### Реализации ad‑hoc на практике

#### 1) ClickHouse (ad‑hoc SQL)

Подходит для быстрого исследования данных в OLAP:

- `clickhouse-client` (CLI),
- HTTP интерфейс (`/` и `/play`),
- BI/Notebook инструменты (Metabase/Superset/Jupyter) через драйвер.

Пример ad‑hoc запроса:

```sql
SELECT
  toDate(event_time) AS d,
  platform,
  uniqExact(user_id) AS dau
FROM analytics.events
WHERE event_time >= now() - INTERVAL 24 HOUR
GROUP BY d, platform
ORDER BY d DESC, platform;
```

Когда это ад‑hoc, а не «продукт»:

- вы не фиксируете модель/витрину,
- запрос не стал частью dbt‑графа/регулярной сборки.

#### 2) Spark (ad‑hoc вычисления)

Подходит, когда нужно:

- сделать тяжёлую обработку «на лету» (join/ML/обогащение),
- читать из lake (S3/ABFS/HDFS) и быстро проверять гипотезы.

Формы:

- `pyspark`/`spark-shell` интерактивно,
- ноутбук (Jupyter/Databricks),
- одноразовый `spark-submit my_job.py --args ...`.

Мини‑пример (ad‑hoc Spark SQL над lake):

```python
df = spark.read.parquet("s3://bucket/events/date=2026-03-04/")
df.createOrReplaceTempView("events")

spark.sql("""
  SELECT platform, count(*) AS c
  FROM events
  WHERE event_name = 'purchase'
  GROUP BY platform
  ORDER BY c DESC
""").show(50, truncate=False)
```

#### 3) dbt и ad‑hoc (что можно и чего нельзя)

dbt — это про **управляемые трансформации**, поэтому ad‑hoc там ограничен по смыслу:

- dbt не лучший инструмент для «поиграться с одним запросом».
- но dbt полезен для «закрепления» ad‑hoc результата в виде модели/витрины/теста.

Практические варианты:

- **`dbt compile`**: быстро посмотреть «во что превратится» модель после Jinja/ref/source.
- **`dbt run -s ...`**: собрать точечно одну модель/подграф (быстрое закрепление).
- **`dbt run-operation`**: выполнить макрос как утилиту (например, создать/почистить временные таблицы).
- **Ephemeral models**: удобно для прототипа логики без физического объекта (но результат всё равно материализуется через «родительскую» модель).

---

### Как превращают ad‑hoc в прод (короткий рецепт)

- **Шаг 1**: ad‑hoc запрос/ноутбук → понимание логики.
- **Шаг 2**: фиксируем в dbt:
  - staging → mart модель,
  - добавляем тесты,
  - добавляем описание/ownership.
- **Шаг 3**: если нужен NRT:
  - ClickHouse MV/Kafka engine или Spark streaming,
  - а dbt — для «более тяжёлых» периодических витрин и контроля качества.

