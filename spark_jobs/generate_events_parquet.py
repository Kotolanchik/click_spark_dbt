import argparse

from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic events to Parquet using Spark.")
    p.add_argument("--out", default="examples/events.parquet", help="Output Parquet path.")
    p.add_argument("--n", type=int, default=100_000, help="Number of events.")
    p.add_argument("--partitions", type=int, default=8, help="Number of Spark partitions to generate.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # SparkSession is provided by spark-submit (spark is available as a global in some shells),
    # but creating it explicitly keeps the script self-contained.
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("generate-events-parquet")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    base = spark.range(0, args.n, 1, numPartitions=args.partitions).withColumnRenamed("id", "row_id")

    # Build a deterministic-ish UUID string from a SHA-256 hex digest:
    # 8-4-4-4-12 format so ClickHouse can cast it to UUID.
    digest = F.sha2(F.concat_ws(":", F.col("row_id").cast("string"), F.rand().cast("string")), 256)
    session_id = F.concat(
        F.substring(digest, 1, 8),
        F.lit("-"),
        F.substring(digest, 9, 4),
        F.lit("-"),
        F.substring(digest, 13, 4),
        F.lit("-"),
        F.substring(digest, 17, 4),
        F.lit("-"),
        F.substring(digest, 21, 12),
    )

    event_name = (
        F.when(F.rand() < 0.70, F.lit("page_view"))
        .when(F.rand() < 0.90, F.lit("add_to_cart"))
        .otherwise(F.lit("purchase"))
    )

    platform = (
        F.when(F.rand() < 0.55, F.lit("web"))
        .when(F.rand() < 0.80, F.lit("ios"))
        .otherwise(F.lit("android"))
    )

    # Random timestamp in the last 7 days (seconds precision; ClickHouse parser will handle it).
    event_time = F.date_format(
        F.expr("current_timestamp() - INTERVAL CAST(rand() * 7 * 24 * 3600 AS INT) seconds"),
        "yyyy-MM-dd HH:mm:ss",
    )

    df = (
        base.withColumn("event_time", event_time)
        .withColumn("user_id", (F.rand() * 100_000).cast("long"))
        .withColumn("session_id", session_id)
        .withColumn("event_name", event_name)
        .withColumn("platform", platform)
        .withColumn("price", F.when(F.col("event_name") == F.lit("purchase"), (F.rand() * 500).cast("double")).otherwise(F.lit(0.0)))
        .withColumn("props_json", F.concat(F.lit('{"path":"/p/'), (F.rand() * 1000).cast("int"), F.lit('","ab":"A"}')))
        .select("event_time", "user_id", "session_id", "event_name", "platform", "price", "props_json")
    )

    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.out)
    )

    spark.stop()


if __name__ == "__main__":
    main()

