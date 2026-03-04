## Apache Spark — архитектура, особенности, примеры

### Что это и где применяется

**Apache Spark** — движок распределённой обработки данных для:

- batch ETL/ELT,
- интерактивной аналитики,
- машинного обучения,
- потоковой обработки (Structured Streaming).

Spark чаще всего используют как «вычислительный слой» поверх Data Lake (S3/ABFS/HDFS) или как ingestion/обогащение перед загрузкой в OLAP (например, ClickHouse).

---

### RDD: определение, возможности, когда нужен

**RDD (Resilient Distributed Dataset)** — базовая абстракция Spark: **распределённая коллекция** элементов, разбитая на **партиции**, с:

- **fault‑tolerance через lineage** (RDD «помнит», какими трансформациями получен, и может быть пересчитан при потере партиции);
- **ленивыми трансформациями** (план строится до action);
- **параллельным выполнением** (по партициям).

Что RDD умеет:

- **Transformations**: `map`, `flatMap`, `filter`, `union`, `distinct`, `keyBy`, `reduceByKey`, `groupByKey`, `join` (в pair‑RDD), `repartition/coalesce`, `sortBy`, `mapPartitions`.
- **Actions**: `count`, `take`, `collect` (опасно на больших данных), `saveAsTextFile`, `foreach`.
- **Persist/Cache**: хранение в памяти/на диске (`MEMORY_ONLY`, `MEMORY_AND_DISK`, …).
- **Контроль партиций**: явный параллелизм, работа с данными «низкого уровня».

Когда RDD действительно уместен (а не DataFrame):

- нужен **произвольный объект**/нестандартная сериализация;
- нужна **тонкая работа с партициями** (`mapPartitions`) или специфические алгоритмы;
- DataFrame API неудобен/невозможен для конкретной операции.

Практика: для ETL/аналитики чаще выбирают **DataFrame/Spark SQL** (Catalyst/Tungsten быстрее), а RDD — «escape hatch».

---

### Архитектура (как устроен Spark)

**Базовая модель**: *Driver* управляет выполнением, *Executors* исполняют задачи на кластере.

- **Driver**:
  - строит логический план вычислений;
  - планирует задачи;
  - держит SparkSession/SparkContext.
- **Executors**:
  - получают задачи (tasks) и выполняют их над партициями данных;
  - кэшируют данные (если включён cache/persist);
  - пишут shuffle-файлы.
- **Job → Stage → Task**:
  - action (`count`, `write`, `collect`) запускает **job**;
  - job делится на **stages** по границам shuffle;
  - stage состоит из **tasks** по числу партиций.

**Ключевой момент**: Spark «ленивый» (lazy). Трансформации строят план, выполнение начинается при action.

---

### Оптимизация (Catalyst + Tungsten)

- **DataFrame/Dataset API** (а не RDD) обычно быстрее, потому что:
  - **Catalyst optimizer** переписывает логический план (pushdown фильтров, упрощения, reorder join’ов);
  - **Whole-stage codegen** генерирует Java-код для операторов;
  - **Tungsten** оптимизирует память/сериализацию и работу с CPU.
- **AQE (Adaptive Query Execution)** может менять план на лету (в зависимости от статистики).

---

### Конфигурирование Spark (что реально настраивают)

#### Где задаются настройки

- **`spark-submit`**: `--master`, `--deploy-mode`, `--conf k=v`, `--packages`, `--jars`.
- **SparkSession/SparkConf** в коде: `.config("spark.sql.shuffle.partitions","200")`.
- **Кластер‑менеджер**: Standalone / YARN / Kubernetes (и их отдельные параметры).

#### Ключевые настройки (краткий смысл)

- **Ресурсы**:
  - `spark.executor.instances` / `spark.dynamicAllocation.enabled`
  - `spark.executor.cores`, `spark.executor.memory`
  - `spark.driver.memory`
- **Шаффл/партиции**:
  - `spark.sql.shuffle.partitions` (число партиций после shuffle в Spark SQL)
  - `spark.default.parallelism` (дефолтный параллелизм для RDD)
- **Сериализация**:
  - `spark.serializer` (часто Kryo в JVM‑сценариях)
- **Broadcast/Join**:
  - `spark.sql.autoBroadcastJoinThreshold`
- **Стриминг**:
  - `spark.sql.streaming.checkpointLocation` (или задаётся в `.option("checkpointLocation", ...)`)
  - `trigger` (processing time), watermark, backpressure (зависит от source)

Мини‑пример запуска с конфигами:

```bash
spark-submit \
  --master local[4] \
  --conf spark.sql.shuffle.partitions=64 \
  --conf spark.default.parallelism=64 \
  spark_jobs/generate_events_parquet.py --n 200000 --out examples/events.parquet
```

---

### Производительность: практические правила

- **Партиционирование важнее «магии»**:
  - слишком мало партиций → недоиспользование кластера;
  - слишком много → оверхед планирования/шедулинга.
- **Shuffle — главный враг**:
  - join/groupBy/Window часто делают shuffle;
  - минимизируйте shuffle (предагрегация, фильтры до join, правильные ключи).
- **Broadcast join**:
  - если одна сторона маленькая, её можно раздать всем executors.
- **Cache/persist**:
  - имеет смысл только если датафрейм используется повторно.

---

### Примеры: batch, streaming, near‑real‑time

#### 1) Batch ETL (файлы → агрегаты → ClickHouse)

```python
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "64")

events = (
    spark.read.parquet("examples/events.parquet")
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("event_date", F.to_date("event_time"))
    .withColumn("price", F.col("price").cast("decimal(12,2)"))
)

daily = (
    events.groupBy("event_date", "platform")
    .agg(
        F.sum(F.when(F.col("event_name") == "purchase", F.col("price")).otherwise(F.lit(0))).alias("revenue"),
        F.sum(F.when(F.col("event_name") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("purchases"),
        F.countDistinct(F.when(F.col("event_name") == "purchase", F.col("user_id"))).alias("buyers"),
    )
)

(
    daily.write.format("jdbc")
    .option("url", "jdbc:clickhouse://localhost:8123/analytics")
    .option("dbtable", "daily_revenue_spark")
    .option("user", "default")
    .option("password", "")
    .mode("append")
    .save()
)
```

#### 2) Streaming (Kafka → микро‑батчи → ClickHouse)

Structured Streaming обычно работает микро‑батчами. Паттерн для ClickHouse — `foreachBatch`:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", LongType()),
    StructField("session_id", StringType()),
    StructField("event_name", StringType()),
    StructField("platform", StringType()),
    StructField("price", DoubleType()),
    StructField("props_json", StringType()),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
    .option("startingOffsets", "latest")
    .load()
)

events = (
    raw.select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
    .select("v.*")
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("event_date", F.to_date("event_time"))
)

def write_to_clickhouse(batch_df, batch_id: int) -> None:
    (
        batch_df.select(
            "event_time", "user_id", "session_id", "event_name", "platform", "price", "props_json"
        )
        .write.format("jdbc")
        .option("url", "jdbc:clickhouse://localhost:8123/analytics")
        .option("dbtable", "events")
        .option("user", "default")
        .option("password", "")
        .mode("append")
        .save()
    )

q = (
    events.writeStream
    .foreachBatch(write_to_clickhouse)
    .option("checkpointLocation", "/tmp/chk/events_to_clickhouse")
    .trigger(processingTime="10 seconds")
    .start()
)
q.awaitTermination()
```

#### 3) Near‑real‑time (NRT): что это и как делают

**Near‑real‑time** в данных — задержка порядка **секунд/десятков секунд/минут**, не «мгновенно».

Типовые реализации:

- **Spark Structured Streaming** с коротким `trigger(processingTime="5-10 seconds")` + `foreachBatch` (как выше).
- **Kafka → ClickHouse напрямую** (Kafka engine) + **Materialized View** для витрин (часто даёт меньшую задержку и меньше движущихся частей).
- **ClickHouse Materialized Views** для инкрементальных агрегатов поверх «сырой» таблицы (подходит для дашбордов).

---

### «Живые» примеры в этом репозитории

- Генерация Parquet через Spark: `spark_jobs/generate_events_parquet.py`
- Сквозной пример: `docs/examples-end-to-end.md`

---

### Как Spark обычно сочетается с ClickHouse и dbt

- **Spark** делает тяжёлую обработку/обогащение/джойны над сырыми данными в lake.
- Результаты кладутся:
  - либо в lake (Parquet), а ClickHouse читает/загружает дальше,
  - либо напрямую в ClickHouse как «raw/staging».
- **dbt** строит витрины и бизнес-слой в ClickHouse (модели, тесты, документация).

