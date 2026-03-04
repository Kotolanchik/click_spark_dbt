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

#### Важный вопрос: «всё в Spark — это RDD?»

Коротко: **нет на уровне API**, но **часто да на уровне исполнения**.

- **Dataset/DataFrame** — это *табличная* абстракция (логический план + оптимизации Catalyst/Tungsten).
- **RDD** — это *низкоуровневая* распределённая коллекция.
- Когда вы вызываете action над **Dataset/DataFrame**, Spark строит физический план, и внутри он часто исполняется как цепочки задач, которые исторически опираются на RDD‑похожий механизм вычисления партиций (но это **не значит**, что вы «работаете с RDD» как с основным API).
- В **Structured Streaming** в режиме микро‑батчей каждый «тик» — это **один batch‑прогон** того же самого Dataset‑пайплайна над новым диапазоном данных. Внутри это снова превращается в задачи по партициям (и да, там будут структуры уровня RDD/Tasks), но снаружи вы пишете `readStream/writeStream`, а не `RDD`.

Практическое правило:

- если у вас `spark.read` / `df.write` → вы в мире batch Dataset/DataFrame;
- если у вас `spark.readStream` / `df.writeStream` → вы в мире streaming (микро‑батчи/continuous), даже если «внутри» оно исполняется батч‑итерациями.

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
spark-shell \
  --master local[4] \
  --conf spark.sql.shuffle.partitions=64 \
  --conf spark.default.parallelism=64 \
  -i spark_jobs/generate_events_parquet.scala
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

Почему это **batch**:

- источник **ограничен** (bounded): конкретная папка Parquet,
- есть начало и конец вычисления: программа отработала и завершилась,
- не нужен checkpoint для продолжения «с места остановки».

Пример на Scala (Dataset/DataFrame API):

```scala
import org.apache.spark.sql.functions._

// Конфиги уровня сессии (можно задавать и через spark-submit/spark-shell --conf)
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "64")

// Bounded source: читаем статичные файлы Parquet -> это batch
val events = spark.read
  .parquet("examples/events.parquet")
  // Ниже — типовые преобразования, это ещё не выполнение (lazy)
  .withColumn("event_time", to_timestamp(col("event_time")))
  .withColumn("event_date", to_date(col("event_time")))
  .withColumn("price", col("price").cast("decimal(12,2)"))

val daily = events
  .groupBy(col("event_date"), col("platform"))
  .agg(
    // Условная агрегация — типичный паттерн для витрин
    sum(when(col("event_name") === lit("purchase"), col("price")).otherwise(lit(0))).as("revenue"),
    sum(when(col("event_name") === lit("purchase"), lit(1)).otherwise(lit(0))).as("purchases"),
    countDistinct(when(col("event_name") === lit("purchase"), col("user_id"))).as("buyers")
  )

// Запись результата: один раз посчитали -> один раз записали -> завершили
// Для JDBC нужен драйвер (обычно через --packages или --jars)
daily.write
  .format("jdbc")
  .option("url", "jdbc:clickhouse://localhost:8123/analytics")
  .option("dbtable", "daily_revenue_spark")
  .option("user", "default")
  .option("password", "")
  .mode("append")
  .save()
```

#### 2) Streaming (Kafka → микро‑батчи → ClickHouse)

Structured Streaming обычно работает микро‑батчами. Паттерн для ClickHouse — `foreachBatch`:

Почему это **streaming**:

- источник **неограничен** (unbounded): Kafka‑топик «идёт бесконечно»,
- приложение **должно жить постоянно** и обрабатывать новые данные,
- нужен **checkpoint**, чтобы при рестарте продолжить обработку корректно,
- Spark выполняет вычисления **микро‑батчами** (каждые \(N\) секунд), но весь режим всё равно streaming.

Пример на Scala (Kafka → foreachBatch → ClickHouse):

```scala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

// Схема JSON-сообщения в Kafka (value)
val schema = StructType(Seq(
  StructField("event_time", StringType),
  StructField("user_id", LongType),
  StructField("session_id", StringType),
  StructField("event_name", StringType),
  StructField("platform", StringType),
  StructField("price", DoubleType),
  StructField("props_json", StringType)
))

// Unbounded source: Kafka -> это streaming
val raw = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "events")
  .option("startingOffsets", "latest")
  .load()

val events = raw
  // Kafka value (bytes) -> string -> JSON -> колонки
  .select(from_json(col("value").cast("string"), schema).as("v"))
  .select("v.*")
  .withColumn("event_time", to_timestamp(col("event_time")))

def writeToClickHouse(batchDf: DataFrame, batchId: Long): Unit = {
  // Внутри foreachBatch у нас обычный batch DataFrame:
  // можно делать любые batch-операции и один раз писать результат
  batchDf
    .select("event_time", "user_id", "session_id", "event_name", "platform", "price", "props_json")
    .write
    .format("jdbc")
    .option("url", "jdbc:clickhouse://localhost:8123/analytics")
    .option("dbtable", "events")
    .option("user", "default")
    .option("password", "")
    .mode("append")
    .save()
}

val q = events.writeStream
  .foreachBatch(writeToClickHouse _)
  // Checkpoint обязателен: хранит прогресс (offsets) и состояние (если есть stateful-операторы)
  .option("checkpointLocation", "/tmp/chk/events_to_clickhouse")
  // Микро-батч раз в 10 секунд: компромисс latency/стоимость
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()

q.awaitTermination()
```

#### 3) Near‑real‑time (NRT): что это и как делают

**Near‑real‑time** в данных — задержка порядка **секунд/десятков секунд/минут**, не «мгновенно».

Почему это **не отдельный режим Spark**, а характеристика пайплайна:

- если вы обрабатываете события каждые 5–10 секунд и пишете в OLAP, то результат «почти real‑time»;
- если раз в час — это уже классический batch.

Типовые реализации:

- **Spark Structured Streaming** с коротким `trigger(processingTime="5-10 seconds")` + `foreachBatch` (как выше).
- **Kafka → ClickHouse напрямую** (Kafka engine) + **Materialized View** для витрин (часто даёт меньшую задержку и меньше движущихся частей).
- **ClickHouse Materialized Views** для инкрементальных агрегатов поверх «сырой» таблицы (подходит для дашбордов).

Мини‑пример NRT‑агрегации в streaming (окна + watermark):

```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

// events: DataFrame из предыдущего примера (readStream)
val nrtAgg = events
  // Watermark нужен, чтобы ограничить состояние при работе с late events
  .withWatermark("event_time", "10 minutes")
  .groupBy(
    window(col("event_time"), "1 minute"),
    col("platform")
  )
  .agg(
    sum(when(col("event_name") === lit("purchase"), col("price")).otherwise(lit(0))).as("revenue_1m")
  )

// Это streaming: агрегаты считаются постоянно, а "near-real-time" задаётся триггером
val q2 = nrtAgg.writeStream
  .outputMode("update")
  .foreachBatch { (batchDf, batchId) =>
    // Каждая минутная "дельта" попадает в ClickHouse почти сразу после завершения микро-батча
    batchDf.write
      .format("jdbc")
      .option("url", "jdbc:clickhouse://localhost:8123/analytics")
      .option("dbtable", "revenue_1m_nrt")
      .option("user", "default")
      .option("password", "")
      .mode("append")
      .save()
  }
  .option("checkpointLocation", "/tmp/chk/revenue_1m_nrt")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()
```

---

### «Живые» примеры в этом репозитории

- Генерация Parquet через Spark (Scala): `spark_jobs/generate_events_parquet.scala`
- Сквозной пример: `docs/examples-end-to-end.md`

---

### Как Spark обычно сочетается с ClickHouse и dbt

- **Spark** делает тяжёлую обработку/обогащение/джойны над сырыми данными в lake.
- Результаты кладутся:
  - либо в lake (Parquet), а ClickHouse читает/загружает дальше,
  - либо напрямую в ClickHouse как «raw/staging».
- **dbt** строит витрины и бизнес-слой в ClickHouse (модели, тесты, документация).

