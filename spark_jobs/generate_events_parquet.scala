import org.apache.spark.sql.functions._

// Этот файл задуман как "spark-shell script":
// пример запуска:
//   N=200000 OUT=examples/events.parquet spark-shell --master local[4] -i spark_jobs/generate_events_parquet.scala
//
// Почему это batch:
// - мы генерируем конечное число строк (bounded dataset),
// - один раз пишем результат в Parquet и завершаем вычисление.

val out = sys.env.getOrElse("OUT", "examples/events.parquet")
val n = sys.env.getOrElse("N", "100000").toLong
val partitions = sys.env.getOrElse("PARTITIONS", "8").toInt

spark.conf.set("spark.sql.session.timeZone", "UTC")

// Генерируем "идентификаторы строк" и синтетические поля событий
val base = spark.range(0, n, 1, partitions).withColumnRenamed("id", "row_id")

// Детерминированный "псевдо-UUID" из sha2: 8-4-4-4-12, чтобы ClickHouse мог cast в UUID
val digest = sha2(concat_ws(":", col("row_id").cast("string"), rand().cast("string")), 256)
val sessionId = concat(
  substring(digest, 1, 8), lit("-"),
  substring(digest, 9, 4), lit("-"),
  substring(digest, 13, 4), lit("-"),
  substring(digest, 17, 4), lit("-"),
  substring(digest, 21, 12)
)

val eventName =
  when(rand() < 0.70, lit("page_view"))
    .when(rand() < 0.90, lit("add_to_cart"))
    .otherwise(lit("purchase"))

val platform =
  when(rand() < 0.55, lit("web"))
    .when(rand() < 0.80, lit("ios"))
    .otherwise(lit("android"))

// Время события за последние 7 дней. Строковый формат удобен для дальнейшего парсинга в ClickHouse.
val eventTime = date_format(
  expr("current_timestamp() - INTERVAL CAST(rand() * 7 * 24 * 3600 AS INT) seconds"),
  "yyyy-MM-dd HH:mm:ss"
)

val df = base
  .withColumn("event_time", eventTime)
  .withColumn("user_id", (rand() * 100000).cast("long"))
  .withColumn("session_id", sessionId)
  .withColumn("event_name", eventName)
  .withColumn("platform", platform)
  // Цена только для purchase, иначе 0
  .withColumn("price", when(col("event_name") === lit("purchase"), (rand() * 500).cast("double")).otherwise(lit(0.0)))
  .withColumn("props_json", concat(lit("""{"path":"/p/"""), (rand() * 1000).cast("int"), lit("""","ab":"A"}""")))
  .select("event_time", "user_id", "session_id", "event_name", "platform", "price", "props_json")

df.write
  .mode("overwrite")
  .option("compression", "snappy")
  .parquet(out)

println(s"Wrote Parquet to: $out (rows: $n, partitions: $partitions)")
