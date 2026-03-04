## Apache Spark — архитектура, особенности, примеры

### Что это и где применяется

**Apache Spark** — движок распределённой обработки данных для:

- batch ETL/ELT,
- интерактивной аналитики,
- машинного обучения,
- потоковой обработки (Structured Streaming).

Spark чаще всего используют как «вычислительный слой» поверх Data Lake (S3/ABFS/HDFS) или как ingestion/обогащение перед загрузкой в OLAP (например, ClickHouse).

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

### «Живые» примеры на PySpark

#### 1) Нормализация событий и дневная выручка

```python
from pyspark.sql import functions as F

events = (
    spark.read.json("examples/events.json")
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("event_date", F.to_date("event_time"))
    .withColumn("price", F.col("price").cast("decimal(12,2)"))
)

daily = (
    events.where(F.col("event_name") == F.lit("purchase"))
    .groupBy("event_date", "platform")
    .agg(
        F.sum("price").alias("revenue"),
        F.count("*").alias("purchases"),
        F.countDistinct("user_id").alias("buyers"),
    )
)
```

#### 2) Запись в ClickHouse через JDBC

Spark может писать в ClickHouse через JDBC (подходит для batch/микробатчей):

```python
clickhouse_url = "jdbc:clickhouse://localhost:8123/analytics"

(
    daily.write
    .format("jdbc")
    .option("url", clickhouse_url)
    .option("dbtable", "daily_revenue_spark")
    .option("user", "default")
    .option("password", "")
    .mode("overwrite")
    .save()
)
```

Практические замечания:

- для больших объёмов важны батч-размеры и параллелизм записи;
- иногда выгоднее писать сначала в Parquet (lake), а затем грузить в ClickHouse более «нативными» способами.

---

### Structured Streaming (в двух словах)

Structured Streaming — модель «таблица, которая постоянно дополняется». В большинстве режимов это микро-батчи:

- источник (Kafka/files),
- трансформации как у batch,
- sink (файлы/таблицы/Kafka и т.п.),
- **checkpoint** обязателен для fault-tolerance.

---

### Как Spark обычно сочетается с ClickHouse и dbt

- **Spark** делает тяжёлую обработку/обогащение/джойны над сырыми данными в lake.
- Результаты кладутся:
  - либо в lake (Parquet), а ClickHouse читает/загружает дальше,
  - либо напрямую в ClickHouse как «raw/staging».
- **dbt** строит витрины и бизнес-слой в ClickHouse (модели, тесты, документация).

