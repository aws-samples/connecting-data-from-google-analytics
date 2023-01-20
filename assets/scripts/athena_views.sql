/*   

Full flattened ga4 events table

*/

CREATE OR REPLACE VIEW "ga4_events_flat" AS 
SELECT DISTINCT
  ga_session_id.ga_session_id
, page_title.page_title
, page_location.page_location
, page_referrer.page_referrer
, percent_scrolled.percent_scrolled
, event_base.*
FROM
  (((((event_base
LEFT JOIN page_location ON (event_base.join_key = page_location.join_key))
LEFT JOIN page_title ON (event_base.join_key = page_title.join_key))
LEFT JOIN page_referrer ON (event_base.join_key = page_referrer.join_key))
LEFT JOIN ga_session_id ON (event_base.join_key = ga_session_id.join_key))
LEFT JOIN percent_scrolled ON (event_base.join_key = percent_scrolled.join_key))

/*   

Base flattened events table and user_property

*/
CREATE OR REPLACE VIEW "event_base" AS 
SELECT DISTINCT
  join_key
, user_prop.value.int_value user_id
, user_prop.value.set_timestamp_micros
, event_date
, event_name
, event_timestamp
, event_previous_timestamp
, event_value_in_usd
, event_bundle_sequence_id
, event_server_timestamp_offset
, user_pseudo_id user_pseudo_id
, stream_id stream_id
, platform platform
, geo.continent
, geo.country
, geo.region
, geo.city
, geo.sub_continent
, geo.metro
, traffic_source.name
, traffic_source.medium
, traffic_source.source
, user_ltv.revenue
, user_ltv.currency
, privacy_info.analytics_storage
, privacy_info.ads_storage
, privacy_info.uses_transient_token
, app_info
, event_dimensions
, user_properties
, event_params
FROM
  "ga4_events"
, UNNEST(user_properties) t (user_prop)


/*   

Page Location from event_params

*/

CREATE OR REPLACE VIEW "page_location" AS 
WITH
  temp AS (
   SELECT
     join_key
   , CAST(eventp.key AS varchar) event_param_key
   , CAST(eventp.value.string_value AS varchar) event_param_value
   , eventp
   FROM
     event_base
   , UNNEST(event_params) t (eventp)
) 
SELECT
  join_key
, (CASE "strpos"(event_param_value, 'http') WHEN 1 THEN eventp.value.string_value END) page_location
FROM
  temp
WHERE ("strpos"(event_param_value, 'http') > 0)


/*   

Page Title from event_params with join_key

*/

CREATE OR REPLACE VIEW "page_title" AS 
WITH
  temp AS (
   SELECT
     join_key
   , CAST(eventp.key AS varchar) event_param_key
   , CAST(eventp.value.string_value AS varchar) event_param_value
   , eventp
   FROM
     event_base
   , UNNEST(event_params) t (eventp)
   WHERE (eventp.key = 'page_title')
) 
SELECT
  join_key
, (CASE event_param_key WHEN 'page_title' THEN eventp.value.string_value END) page_title
FROM
  temp


/*   

page_location from event_params

*/

CREATE OR REPLACE VIEW "page_location" AS 
WITH
  temp AS (
   SELECT
     event_params
   , join_key
   , CAST(eventp.key AS varchar) event_param_key
   , CAST(eventp.value.string_value AS varchar) event_param_value
   , eventp
   FROM
     event_base
   , UNNEST(event_params) t (eventp)
   WHERE (eventp.key = 'page_location')
) 
SELECT DISTINCT
  join_key
, (CASE event_param_key WHEN 'page_location' THEN eventp.value.string_value END) page_location
FROM
  temp
WHERE ("strpos"(event_param_value, 'http') > 0)


/*   

page_referrer from event_params

*/

CREATE OR REPLACE VIEW "page_referrer" AS 
WITH
  temp AS (
   SELECT
     event_params
   , join_key
   , CAST(eventp.key AS varchar) event_param_key
   , CAST(eventp.value.string_value AS varchar) event_param_value
   , eventp
   FROM
     event_base
   , UNNEST(event_params) t (eventp)
   WHERE (eventp.key = 'page_referrer')
) 
SELECT DISTINCT
  join_key
, (CASE event_param_key WHEN 'page_referrer' THEN eventp.value.string_value END) page_referrer
FROM
  temp
WHERE ("strpos"(event_param_value, 'http') > 0)

/*   

ga_session_id from event_params

*/

CREATE OR REPLACE VIEW "ga_session_id" AS 
WITH
  temp AS (
   SELECT
     join_key
   , CAST(eventp.key AS varchar) event_param_key
   , CAST(eventp.value.int_value AS varchar) event_param_value
   , eventp
   FROM
     event_base
   , UNNEST(event_params) t (eventp)
   WHERE (eventp.key = 'ga_session_id')
) 
SELECT DISTINCT
  join_key
, (CASE event_param_key WHEN 'ga_session_id' THEN eventp.value.int_value END) ga_session_id
FROM
  temp


/*   

Page percent_scrolled from event_params

*/

CREATE OR REPLACE VIEW "percent_scrolled" AS 
WITH
  temp AS (
   SELECT
     join_key
   , CAST(eventp.key AS varchar) event_param_key
   , CAST(eventp.value.int_value AS varchar) event_param_value
   , eventp
   FROM
     event_base
   , UNNEST(event_params) t (eventp)
      WHERE (eventp.key = 'percent_scrolled')
) 
SELECT
  join_key
, (CASE event_param_key WHEN 'percent_scrolled' THEN eventp.value.int_value END) percent_scrolled
FROM
  temp