-- start_level
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.start_level`
PARTITION BY event_date
CLUSTER BY level, version, country AS
SELECT
    user_pseudo_id,
    date_par as event_date,
    version,
    country,
    event_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'level') as level
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'start_level';

-- revive_level
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.revive_level`
PARTITION BY event_date
CLUSTER BY level AS
SELECT
    user_pseudo_id,
    date_par as event_date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'level') as level,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'percent') as percent,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'lose_type') as lose_type
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'revive_level';

-- win_level
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.win_level`
PARTITION BY event_date
CLUSTER BY level, version, country AS
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'level') as level
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'win_level';


-- lose_level
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.lose_level`
PARTITION BY event_date
CLUSTER BY level, version, country AS
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'level') as level
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'lose_level';

-- booster_used
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.booster_used`
PARTITION BY event_date
CLUSTER BY  level, booster_type, version, country AS
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'level') as level,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'booster_type') as booster_type
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'booster_used';

-- in_app_purchase
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.in_app_purchase`
PARTITION BY event_date AS
SELECT
    user_pseudo_id,
    date_par as event_date,
    version, country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'product_id') as product_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'price') as price,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency') as currency,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'quantity') as quantity,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value') as value,
    event_value_in_usd
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'in_app_purchase';

-- ad_impression
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.ad_impression`
PARTITION BY event_date 
CLUSTER BY ad_platform, version, country AS
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_platform') as ad_platform,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_format') as ad_format,
    (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') as value,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_source') as ad_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency') as currency
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'ad_impression';

-- ads_reward_complete
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.ads_reward_complete`
PARTITION BY event_date AS
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'placement') as placement
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'ads_reward_complete';

-- first_open
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.first_open`
PARTITION BY event_date as
SELECT
    user_pseudo_id,
    date_par as event_date, version, country
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'first_open';

-- session_start
CREATE OR REPLACE TABLE  `royal-hexa-in-house.vtd_flatten_data.session_start`
PARTITION BY event_date as
SELECT
  user_pseudo_id, version, country,
  date_par as event_date
FROM `royal-hexa-in-house.pixon_data_science.004_full`   WHERE event_name = 'session_start';

-- app_remove
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.app_remove`
PARTITION BY event_date as
SELECT
  'app_remove' AS event_name,
  user_pseudo_id,
  date_par as event_date
FROM `royal-hexa-in-house.pixon_data_science.004_full`   WHERE event_name = 'app_remove';

-- user_engagement
CREATE OR REPLACE TABLE `royal-hexa-in-house.vtd_flatten_data.user_engagement`
PARTITION BY event_date AS
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') as engagement_time_msec
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'user_engagement';




























