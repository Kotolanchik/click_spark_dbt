CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events
(
  event_date Date DEFAULT toDate(event_time),
  event_time DateTime64(3, 'UTC'),
  user_id    UInt64,
  session_id UUID,
  event_name LowCardinality(String),
  platform   LowCardinality(String),
  price      Decimal(12, 2),
  props_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_name, user_id);

