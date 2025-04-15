-- start_level
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.start_level`
SELECT
    user_pseudo_id,
    date_par as event_date,
    version,
    country,
    event_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'level') as level
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'start_level'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- revive_level
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.revive_level`
SELECT
    user_pseudo_id,
    date_par as event_date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'level') as level,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'percent') as percent,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'lose_type') as lose_type
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'revive_level'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- win_level
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.win_level`
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'level') as level
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'win_level'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- lose_level
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.lose_level`
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'level') as level
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'lose_level'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- booster_used
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.booster_used`
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'level') as level,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'booster_type') as booster_type
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'booster_used'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- in_app_purchase
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.in_app_purchase`
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
WHERE event_name = 'in_app_purchase'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- ad_impression
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.ad_impression`
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_platform') as ad_platform,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_format') as ad_format,
    (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') as value,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_source') as ad_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'currency') as currency
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'ad_impression'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- ads_reward_complete
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.ads_reward_complete`
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'placement') as placement
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'ads_reward_complete'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- first_open
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.first_open`
SELECT
    user_pseudo_id,
    date_par as event_date, version, country
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'first_open'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- session_start
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.session_start`
SELECT
  user_pseudo_id, version, country,
  date_par as event_date
FROM `royal-hexa-in-house.pixon_data_science.004_full`   
WHERE event_name = 'session_start'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- app_remove
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.app_remove`
SELECT
  'app_remove' AS event_name,
  user_pseudo_id,
  date_par as event_date
FROM `royal-hexa-in-house.pixon_data_science.004_full`   
WHERE event_name = 'app_remove'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);

-- user_engagement
INSERT INTO `royal-hexa-in-house.vtd_flatten_data.user_engagement`
SELECT
    user_pseudo_id,
    date_par as event_date, version, country,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') as engagement_time_msec
FROM `royal-hexa-in-house.pixon_data_science.004_full`
WHERE event_name = 'user_engagement'
AND date_par > (select max(date_par) from `royal-hexa-in-house.pixon_data_science.004_full`);



























