CREATE DATABASE IF NOT EXISTS analytics;

-- Таблица под near-real-time агрегаты из Spark.
-- ReplacingMergeTree позволяет писать "обновления" (update mode) как новые версии строк.
-- Ключ: (window_start, platform), версия: updated_at.
CREATE TABLE IF NOT EXISTS analytics.revenue_1m_nrt
(
  window_start DateTime('UTC'),
  window_end   DateTime('UTC'),
  platform     LowCardinality(String),
  revenue_1m   Decimal(18, 2),
  purchases_1m UInt64,
  buyers_1m    UInt64,
  updated_at   DateTime('UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (window_start, platform);

