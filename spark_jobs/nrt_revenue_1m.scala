import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

// Near-real-time (NRT) пример на Structured Streaming.
//
// Почему это near-real-time:
// - источник событий неограничен (streaming),
// - обработка идёт микро-батчами с маленьким интервалом (trigger),
// - задержка выхода результата ~ интервал trigger + время обработки.
//
// Запуск (самый простой, без Kafka, источник rate, вывод в консоль):
//   TRIGGER="5 seconds" SOURCE=rate SINK=console spark-shell --master local[2] -i spark_jobs/nrt_revenue_1m.scala
//
// Запуск с Kafka (если есть Kafka):
//   SOURCE=kafka BOOTSTRAP=localhost:9092 TOPIC=events TRIGGER="10 seconds" SINK=console \
//   spark-shell --master local[2] -i spark_jobs/nrt_revenue_1m.scala
//
// Запуск с записью в ClickHouse (нужен JDBC драйвер и таблица-приёмник):
//   SOURCE=rate SINK=clickhouse CLICKHOUSE_URL="jdbc:clickhouse://localhost:8123/analytics" \
//   CLICKHOUSE_TABLE="revenue_1m_nrt" TRIGGER="5 seconds" \
//   spark-shell --master local[2] -i spark_jobs/nrt_revenue_1m.scala
//
// Важно про запись агрегатов:
// - агрегаты по окнам в streaming обычно обновляются (update mode), поэтому "append в обычную таблицу"
//   приведёт к дубликатам/переобновлениям.
// - один из практичных вариантов для ClickHouse: писать обновления в таблицу на ReplacingMergeTree(updated_at),
//   тогда "последняя версия" строки по (window_start, platform) останется актуальной.

val source = sys.env.getOrElse("SOURCE", "rate")            // rate | kafka
val sink = sys.env.getOrElse("SINK", "console")             // console | clickhouse
val triggerEvery = sys.env.getOrElse("TRIGGER", "10 seconds")
val checkpoint = sys.env.getOrElse("CHECKPOINT", "/tmp/chk/nrt_revenue_1m")

spark.conf.set("spark.sql.session.timeZone", "UTC")

def readFromRate(): DataFrame = {
  val rowsPerSecond = sys.env.getOrElse("RPS", "50")
  val raw = spark.readStream
    .format("rate")
    .option("rowsPerSecond", rowsPerSecond)
    .load()

  // rate source генерирует (timestamp, value). Превратим это в "события".
  raw.select(
    col("timestamp").as("event_time"),
    (col("value") % 100000).cast("long").as("user_id"),
    // "UUID-похожая" строка (для примера; не обязана быть валидным UUID)
    sha2(col("value").cast("string"), 256).as("session_id"),
    when(col("value") % 10 === 0, lit("purchase"))
      .when(col("value") % 10 === 1, lit("add_to_cart"))
      .otherwise(lit("page_view")).as("event_name"),
    when(col("value") % 3 === 0, lit("web"))
      .when(col("value") % 3 === 1, lit("ios"))
      .otherwise(lit("android")).as("platform"),
    when(col("value") % 10 === 0, (rand() * 500).cast("double")).otherwise(lit(0.0)).as("price"),
    lit("""{"nrt":"rate"}""").as("props_json")
  )
}

def readFromKafka(): DataFrame = {
  val bootstrap = sys.env.getOrElse("BOOTSTRAP", "localhost:9092")
  val topic = sys.env.getOrElse("TOPIC", "events")

  val schema = StructType(Seq(
    StructField("event_time", StringType),
    StructField("user_id", LongType),
    StructField("session_id", StringType),
    StructField("event_name", StringType),
    StructField("platform", StringType),
    StructField("price", DoubleType),
    StructField("props_json", StringType)
  ))

  val raw = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .load()

  raw.select(from_json(col("value").cast("string"), schema).as("v"))
    .select("v.*")
    .withColumn("event_time", to_timestamp(col("event_time")))
}

val events =
  if (source == "kafka") readFromKafka()
  else readFromRate()

// NRT агрегат: минутное окно + watermark для ограничения состояния (late events)
val nrtAgg = events
  .withWatermark("event_time", "10 minutes")
  .groupBy(
    window(col("event_time"), "1 minute").as("w"),
    col("platform")
  )
  .agg(
    sum(when(col("event_name") === lit("purchase"), col("price")).otherwise(lit(0))).as("revenue_1m"),
    sum(when(col("event_name") === lit("purchase"), lit(1)).otherwise(lit(0))).as("purchases_1m"),
    countDistinct(when(col("event_name") === lit("purchase"), col("user_id"))).as("buyers_1m")
  )
  .select(
    col("w.start").as("window_start"),
    col("w.end").as("window_end"),
    col("platform"),
    col("revenue_1m"),
    col("purchases_1m"),
    col("buyers_1m"),
    current_timestamp().as("updated_at")
  )

def writeBatchToClickHouse(batchDf: DataFrame, batchId: Long): Unit = {
  val url = sys.env.getOrElse("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/analytics")
  val table = sys.env.getOrElse("CLICKHOUSE_TABLE", "revenue_1m_nrt")
  val user = sys.env.getOrElse("CLICKHOUSE_USER", "default")
  val password = sys.env.getOrElse("CLICKHOUSE_PASSWORD", "")

  // batchDf — это "срез обновлений" за микро-батч.
  // Если ClickHouse таблица настроена как ReplacingMergeTree(updated_at),
  // то новые версии по (window_start, platform) заменят старые при мерджах.
  batchDf.write
    .format("jdbc")
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .mode("append")
    .save()
}

val writer = nrtAgg.writeStream
  // Агрегации по окнам обычно обновляются -> корректный режим вывода: update
  .outputMode("update")
  .option("checkpointLocation", checkpoint)
  // Именно trigger задаёт "near-real-time": чем меньше интервал, тем меньше latency (и выше нагрузка)
  .trigger(Trigger.ProcessingTime(triggerEvery))

val q =
  if (sink == "clickhouse") writer.foreachBatch(writeBatchToClickHouse _).start()
  else writer.format("console").option("truncate", "false").start()

q.awaitTermination()

