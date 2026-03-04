## ClickHouse — архитектура, особенности, примеры

### Что это и где применяется

**ClickHouse** — колоночная OLAP-СУБД, оптимизированная для:

- больших объёмов данных (миллиарды строк),
- быстрых агрегаций/срезов,
- высокой параллельности запросов,
- дешёвого хранения (компрессия + колоночный формат).

Типичные кейсы: продуктовая/маркетинговая аналитика, логи/метрики/трейсы, витрины для BI, near-real-time дашборды.

---

### Архитектура (как оно устроено)

- **Колонки, а не строки**: данные хранятся по столбцам, поэтому `SELECT sum(x)` читает только нужные колонки.
- **Табличные движки** (table engines) определяют физику хранения и поведения таблицы.
  - Самый важный класс — семейство **MergeTree**.
- **Партиции → парты (parts) → мерджи (merges)**:
  - вставки создают новые *parts*;
  - в фоне ClickHouse *сливает* parts в более крупные (мерджи);
  - это ключ к высокой скорости вставок и чтения, но требует понимания ключей и партиционирования.
- **Ключи в MergeTree**:
  - **`ORDER BY`** — сортировочный ключ (определяет физический порядок данных и ускоряет фильтры по префиксу);
  - **`PRIMARY KEY`** в MergeTree по сути задаёт индекс по сортировочному ключу (разреженный индекс), а не уникальность как в OLTP;
  - **`PARTITION BY`** — разбиение на независимые сегменты (удаление/TTL, эффективность мерджей).
- **Запросный движок**:
  - запрос компилируется в *pipeline* операторов;
  - распараллеливается по частям данных;
  - активно использует векторизацию, SIMD, компрессию.
- **Кластеризация** (по желанию):
  - репликация: `ReplicatedMergeTree` (координация через ClickHouse Keeper/ZooKeeper);
  - шардинг: `Distributed` таблицы для распараллеливания чтения/записи между шардами.

---

### Ключевые особенности и «подводные камни»

- **`ORDER BY` — самое важное решение**: неправильный сортировочный ключ ведёт к чтению «всего подряд» и деградации.
- **Много мелких вставок = много parts**: хуже мерджи/метаданные → используйте батчи, буферы, Kafka engine/Materialized Views, или staging таблицы.
- **Высокая кардинальность**:
  - `LowCardinality(String)` экономит место/ускоряет;
  - но не всегда (сильно уникальные значения не выигрывают).
- **Join’ы**:
  - ClickHouse силён в агрегациях, но join’ы нужно проектировать: распределённость, `ANY`/`ALL`, предварительные агрегации, словари, materialized views.
- **Мутации (`ALTER UPDATE/DELETE`)** — дорогие: это фоновые переписывания parts. В аналитике чаще используют *insert-only* + `ReplacingMergeTree`/`CollapsingMergeTree`/версии.
- **Изоляция/транзакции**: ClickHouse не OLTP; транзакционность ограничена (зависит от версии/фич).

---

### Базовые примеры (DDL/DML)

#### 1) Таблица событий (MergeTree)

```sql
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
ORDER BY (event_date, event_name, user_id)
SETTINGS index_granularity = 8192;
```

Почему так:

- `PARTITION BY toYYYYMM(...)` удобно для TTL/удаления по месяцам и ускоряет «сканы по времени».
- `ORDER BY (date, event, user)` ускоряет фильтры по дате/ивенту/пользователю.

#### 2) Вставка данных батчем

```sql
INSERT INTO analytics.events (event_time, user_id, session_id, event_name, platform, price, props_json)
VALUES
  (now64(3), 1, generateUUIDv4(), 'page_view', 'web', 0, '{"path":"/"}'),
  (now64(3), 1, generateUUIDv4(), 'purchase',  'web', 199.90, '{"sku":"A1"}');
```

#### 3) Типовой аналитический запрос

```sql
SELECT
  event_date,
  event_name,
  uniqExact(user_id) AS users,
  count()            AS events,
  sum(price)         AS revenue
FROM analytics.events
WHERE event_date >= today() - 7
GROUP BY event_date, event_name
ORDER BY event_date, event_name;
```

---

### Материализованные представления (ускорение витрин)

Идея: не «пересчитывать всё на лету», а **инкрементально** накапливать агрегаты.

```sql
CREATE TABLE IF NOT EXISTS analytics.daily_revenue
(
  event_date Date,
  platform   LowCardinality(String),
  revenue    Decimal(18, 2)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, platform);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_daily_revenue
TO analytics.daily_revenue
AS
SELECT
  toDate(event_time) AS event_date,
  platform,
  sum(price) AS revenue
FROM analytics.events
WHERE event_name = 'purchase'
GROUP BY event_date, platform;
```

---

### TTL и управление жизненным циклом

```sql
ALTER TABLE analytics.events
  MODIFY TTL event_date + INTERVAL 180 DAY DELETE;
```

---

### Практики проектирования (короткий чеклист)

- **Сначала определите основные фильтры** (обычно время + разрезы) → это кандидат на `ORDER BY`.
- **Партиционируйте только по тому, что реально помогает** (время — почти всегда). Слишком много партиций = оверхед.
- **Для обновлений используйте подходы insert-only**:
  - `ReplacingMergeTree(version)` для «последней версии» строки;
  - `CollapsingMergeTree(sign)` для событий «+/-».
- **Следите за parts/merges**: много мелких parts — ранний симптом проблем с ingestion.

