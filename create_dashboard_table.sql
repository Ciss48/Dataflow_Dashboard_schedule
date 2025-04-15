-- new user by day, version, platform 
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_dashboard_data.new_user`
PARTITION BY event_date
CLUSTER BY  country,version AS
select 
  event_date,
  country,
  version,
  count(distinct user_pseudo_id) as new_user
from `royal-hexa-in-house.vtd_flatten_data.first_open` 
group by  event_date,country, version;

-- dau
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_dashboard_data.dau`
PARTITION BY event_date
CLUSTER BY  country,version AS
select 
  event_date,
  country,
  version,
  count(distinct fo.user_pseudo_id) as dau
from `royal-hexa-in-house.vtd_flatten_data.session_start` fo 
group by  event_date,country, version;

-- playtime
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_dashboard_data.playtime`
PARTITION BY event_date
CLUSTER BY  country,version,platform AS
WITH session_data AS (
  SELECT
    user_pseudo_id,
    version,
    date_par,
    platform,
    country,
    (SELECT SAFE_CAST(value.int_value AS INT64)
     FROM UNNEST(event_params)
     WHERE key = 'engagement_time_msec') / 1000 AS session_duration_seconds,   
    (SELECT SAFE_CAST(value.int_value AS INT64)
     FROM UNNEST(event_params)
     WHERE key = 'ga_session_id') AS session_id,
    PARSE_DATE('%Y%m%d', event_date) AS event_date
  FROM `royal-hexa-in-house.pixon_data_science.004_full`
  WHERE event_name IN ('user_engagement', 'session_start', 'first_open', 'screen_view')
),
playtime_metrics AS (
  SELECT
    version,
    date_par,
    platform,
    country,
    COUNT(DISTINCT user_pseudo_id) AS cohort_size,
    COUNT(DISTINCT session_id) AS total_sessions,
    ROUND(AVG(session_duration_seconds) / 60, 2) AS avg_session_length_minutes,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT session_id), COUNT(DISTINCT user_pseudo_id)), 2) AS avg_sessions_per_user,
    ROUND(SAFE_DIVIDE(SUM(session_duration_seconds), 3600), 2) AS total_playtime_hours,
    ROUND(SAFE_DIVIDE(SUM(session_duration_seconds), COUNT(DISTINCT user_pseudo_id)) / 60, 2) AS avg_playtime_per_user_minutes,
    ROUND(SAFE_DIVIDE(100.0 * SUM(CASE WHEN session_duration_seconds >= 300 THEN 1 ELSE 0 END), COUNT(session_id)), 2) AS pct_long_sessions
  FROM session_data
  WHERE session_duration_seconds IS NOT NULL
  GROUP BY version, date_par, platform, country
)
SELECT
  version,
  date_par as event_date,
  platform,
  country,
  total_sessions,
  avg_session_length_minutes,
  avg_sessions_per_user,
  avg_playtime_per_user_minutes
FROM playtime_metrics;


-- Cohort
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_dashboard_data.cohort`
PARTITION BY event_date
CLUSTER BY  country,version AS
WITH first_open_users AS (
SELECT user_pseudo_id, event_date AS cohort_date
  FROM `royal-hexa-in-house.vtd_flatten_data.first_open`
),
session_users AS (
  SELECT user_pseudo_id, event_date
  FROM `royal-hexa-in-house.vtd_flatten_data.session_start`
),
user_info AS (
  SELECT user_pseudo_id, event_date, version, country
  FROM `royal-hexa-in-house.vtd_flatten_data.user_data`
),
joined_first_open AS (
  SELECT
    f.user_pseudo_id,
    f.cohort_date,
    u.version,
    u.country
  FROM first_open_users f
  LEFT JOIN user_info u
  ON f.user_pseudo_id = u.user_pseudo_id AND f.cohort_date = u.event_date
),
joined_sessions AS (
  SELECT
    s.user_pseudo_id,
    s.event_date,
    u.version,
    u.country
  FROM session_users s
  LEFT JOIN user_info u
  ON s.user_pseudo_id = u.user_pseudo_id AND s.event_date = u.event_date
),
retention_calc AS (
  SELECT
    f.cohort_date,
    s.event_date,
    f.version,
    f.country,
    DATE_DIFF(s.event_date, f.cohort_date, DAY) AS days_since_first_open,
    COUNT(DISTINCT s.user_pseudo_id) AS retained_user
  FROM joined_first_open f
  JOIN joined_sessions s
    ON f.user_pseudo_id = s.user_pseudo_id
   AND DATE_DIFF(s.event_date, f.cohort_date, DAY) BETWEEN 0 AND 30
   AND f.version IS NOT NULL AND f.country IS NOT NULL
  GROUP BY f.cohort_date, s.event_date, f.version, f.country
),
cohort_base AS (
  SELECT
    cohort_date,
    version,
    country,
    COUNT(DISTINCT user_pseudo_id) AS total_users
  FROM joined_first_open
  GROUP BY cohort_date, version, country
),
retention_final AS (
  SELECT
    r.cohort_date as event_date,
    r.days_since_first_open,
    r.version,
    r.country,
    c.total_users,
    r.retained_user,
    ROUND(SAFE_DIVIDE(r.retained_user, c.total_users) * 100, 2) AS retention_rate
  FROM retention_calc r
  JOIN cohort_base c
    ON r.cohort_date = c.cohort_date
   AND r.version = c.version
   AND r.country = c.country
)
SELECT *
FROM retention_final;


-- rev_iap_ads_3
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_dashboard_data.rev_iap_ads3`
PARTITION BY event_date
CLUSTER BY version, country AS
WITH daily_metrics AS (
  SELECT 
    e.event_date,
    e.version,
    e.country,
    COUNT(DISTINCT e.user_pseudo_id) as total_user,
    COUNT(DISTINCT CASE WHEN a.ad_format = 'REWARDED' THEN a.user_pseudo_id END) as ad_rw_count,
    COUNT(DISTINCT CASE WHEN a.ad_format = 'INTER' THEN a.user_pseudo_id END) as ad_inter_count,
    COALESCE(SUM(a.value), 0) as ads_rev,
    (COALESCE(SUM(i.event_value_in_usd), 0)) as iap_rev
  FROM (
    SELECT event_date, version, country, COUNT(DISTINCT user_pseudo_id) AS total_user, user_pseudo_id
    FROM `royal-hexa-in-house.vtd_flatten_data.user_engagement`
    GROUP BY event_date, version, country, user_pseudo_id
    ) e
  LEFT JOIN (
    SELECT user_pseudo_id, version, country, event_date, SUM(value) AS value, ad_format
    FROM `royal-hexa-in-house.vtd_flatten_data.ad_impression`
    GROUP BY user_pseudo_id, version, country, event_date, ad_format
    ) a
    ON e.user_pseudo_id = a.user_pseudo_id
        AND e.event_date = a.event_date
        AND e.version = a.version
        AND e.country = a.country
  LEFT JOIN (
    SELECT user_pseudo_id, version, country, event_date, SUM(event_value_in_usd) as event_value_in_usd
    FROM `royal-hexa-in-house.vtd_flatten_data.in_app_purchase`
    GROUP BY user_pseudo_id, version, country, event_date) i
    ON e.user_pseudo_id = i.user_pseudo_id
        AND e.event_date = i.event_date
        AND e.version = i.version
        AND e.country = i.country
  GROUP BY e.event_date, e.version, e.country
)
SELECT 
  event_date,
  version,
  country,
  iap_rev,
  ads_rev,
  (iap_rev + ads_rev) as total_revenue,
  total_user,
  ad_rw_count,
  ad_inter_count
FROM daily_metrics;


-- winrate2 
create or replace table `royal-hexa-in-house.vtd_dashboard_data.winrate2`
partition by event_date 
cluster by version as 
WITH combined_data AS (
  SELECT 
    s.event_date,
    s.version,
    s.level,
    s.user_pseudo_id,
    COUNT(DISTINCT s.user_pseudo_id) as user_start,
    COUNT(DISTINCT w.user_pseudo_id) as event_win,
    COUNT(DISTINCT l.user_pseudo_id) as event_lose
  FROM `royal-hexa-in-house.vtd_flatten_data.start_level` s
  LEFT JOIN `royal-hexa-in-house.vtd_flatten_data.win_level` w
    ON s.user_pseudo_id = w.user_pseudo_id 
    AND s.event_date = w.event_date 
    AND s.version = w.version 
    AND s.level = w.level
  LEFT JOIN `royal-hexa-in-house.vtd_flatten_data.lose_level` l
    ON s.user_pseudo_id = l.user_pseudo_id 
    AND s.event_date = l.event_date 
    AND s.version = l.version 
    AND s.level = l.level
  WHere s.user_pseudo_id in (select distinct user_pseudo_id from `royal-hexa-in-house.vtd_flatten_data.first_open`)
  GROUP BY s.event_date, s.version, s.level, s.user_pseudo_id
)
SELECT 
  event_date,
  version,
  safe_cast(level as int) as level,
  SUM(user_start) as user_start,
  SUM(event_win) as event_win,
  SUM(event_lose) as event_lose,
  ROUND(SAFE_DIVIDE(SUM(event_win), SUM(event_win) + SUM(event_lose)) * 100, 2) as winrate
FROM combined_data
GROUP BY event_date, version, level;

-- -- droprate 3
-- create or replace table `royal-hexa-in-house.vtd_dashboard_data.drop_rate3`
-- partition by event_date
-- cluster by version as 
-- WITH first_start_per_level AS (
--   SELECT 
--     event_date,
--     version,
--     user_pseudo_id,
--     CAST(level AS INT64) AS level,
--     ROW_NUMBER() OVER (
--       PARTITION BY event_date, version, user_pseudo_id, CAST(level AS INT64)
--       ORDER BY event_timestamp -- giả định bạn có cột timestamp
--     ) AS rn
--   FROM `royal-hexa-in-house.vtd_flatten_data.start_level`
-- ),
-- distinct_start_level AS (
--   SELECT *
--   FROM first_start_per_level
--   WHERE rn = 1
-- ),
-- level_stats AS (
--   SELECT 
--     event_date,
--     version,
--     level,
--     COUNT(DISTINCT user_pseudo_id) as user_start
--   FROM distinct_start_level
--   GROUP BY event_date, version, level
-- ),
-- next_level_stats AS (
--   SELECT 
--     next_level.event_date,
--     next_level.version,
--     next_level.level - 1 as current_level,
--     COUNT(DISTINCT next_level.user_pseudo_id) as user_start_next_level
--   FROM distinct_start_level next_level
--   JOIN distinct_start_level prev_level
--     ON next_level.user_pseudo_id = prev_level.user_pseudo_id
--     AND next_level.event_date = prev_level.event_date
--     AND next_level.version = prev_level.version
--     AND next_level.level = prev_level.level + 1
--   GROUP BY next_level.event_date, next_level.version, next_level.level - 1
-- )
-- SELECT 
--   l.event_date,
--   l.version,
--   l.level,
--   l.user_start,
--   COALESCE(n.user_start_next_level, 0) as user_start_next_level,
--   ROUND(SAFE_DIVIDE(COALESCE(n.user_start_next_level, 0), l.user_start) * 100, 2) as retention_rate
-- FROM level_stats l
-- LEFT JOIN next_level_stats n
--   ON l.event_date = n.event_date 
--   AND l.version = n.version 
--   AND l.level = n.current_level;


-- level_booster
CREATE or replace TABLE `royal-hexa-in-house.vtd_dashboard_data.level_booster2`
PARTITION BY event_date
CLUSTER BY level,version AS
with b as (
SELECT
  event_date,
  version,
  SAFE_CAST(level AS INT64) AS level,
  COUNT(*) AS num_booster,
FROM `royal-hexa-in-house.vtd_flatten_data.booster_used` 
group by event_date, version, level
), u as
(SELECT event_date,version, SAFE_CAST(level AS INT64) AS level,
  COUNT( distinct user_pseudo_id) AS user_start
  from `royal-hexa-in-house.vtd_flatten_data.start_level`
  group by event_date, version, level) 
select u.event_date, u.version, u.level, b.num_booster, u.user_start
from u left join b on b.event_date = u.event_date and b.version = u.version and b.level = u.level;

-- booster_type
CREATE or replace TABLE `royal-hexa-in-house.vtd_dashboard_data.booster_type`
PARTITION BY event_date
CLUSTER BY version AS
SELECT
  event_date,
  version,
  booster_type,
  COUNT(*) AS num_booster_used
FROM `royal-hexa-in-house.vtd_flatten_data.booster_used` 
GROUP BY event_date, version, booster_type;


--ads_inter_rw_cnt
CREATE or replace TABLE `royal-hexa-in-house.vtd_dashboard_data.ads_inter_rw_cnt`
PARTITION BY event_date
CLUSTER BY country, version AS 
  SELECT 
    e.event_date,
    e.version,
    e.country,
    COUNT(DISTINCT e.user_pseudo_id) as total_user,
    COUNT(DISTINCT CASE WHEN a.ad_format = 'REWARDED' THEN a.user_pseudo_id END) as ad_rw_count,
    COUNT(DISTINCT CASE WHEN a.ad_format = 'INTER' THEN a.user_pseudo_id END) as ad_inter_count,
  FROM `royal-hexa-in-house.vtd_flatten_data.user_engagement` e
  LEFT JOIN `royal-hexa-in-house.vtd_flatten_data.ad_impression` a
    ON e.user_pseudo_id = a.user_pseudo_id
    AND e.event_date = a.event_date
    AND e.version = a.version
    AND e.country = a.country
  GROUP BY e.event_date, e.version, e.country;

-- drop 
CREATE or replace TABLE `royal-hexa-in-house.vtd_dashboard_data.drop_final`
PARTITION BY event_date
CLUSTER BY level, version AS 
with level_stats AS (
  SELECT 
    event_date,
    version,
    cast(level as int64) as level,
    COUNT(DISTINCT user_pseudo_id) as user_start
  FROM `royal-hexa-in-house.vtd_flatten_data.start_level`
  GROUP BY event_date, version, level
),
next_level_stats AS (
  SELECT 
    a.event_date,
    a.version,
    b.level - 1 as level,
    b.user_start as user_start_next_level
  FROM level_stats a join level_stats b
  on a.event_date = b.event_date
  and a.version = b.version
  and a.level = b.level - 1
)
select 
  a.event_date,
  a.version,
  a.level,
  a.user_start,
  case when b.user_start_next_level >= a.user_start then a.user_start - 1 else b.user_start_next_level end as user_start_next_level
 from level_stats a left join next_level_stats b
on a.event_date = b.event_date
and a.version = b.version
and a.level = b.level

