/*   

Get all the events flattened

*/


SELECT DISTINCT
  join_key
, page_title
, page_location
, ga_session_id
, page_referrer
, percent_scrolled
, session_engaged
, user_prop.value.int_value user_id
, user_prop.value.set_timestamp_micros
, event_date
, event_name
, event_timestamp
, event_previous_timestamp
, event_value_in_usd
, event_bundle_sequence_id
, event_server_timestamp_offset
, user_pseudo_id
, stream_id 
, platform
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

Get the users who performed a Click Event

*/


SELECT
  page_title,
  event_name,
  event_date,
  user_id
FROM
  "ga4_analytics"."event_base" 
WHERE
  event_name LIKE '%Click%'


/*   

Get the distinct Event Names

*/


SELECT distinct event_name 
FROM "ga4_analytics"."event_base" 

/*   

Last 7 day events

*/


SELECT * FROM "ga4_analytics"."event_base_title_location" 
where
cast(date_parse(event_date,'%Y%m%d') as date) > current_date - interval '7' day

/*   

Yesterday's events

*/

SELECT * FROM "ga4_analytics"."event_base_title_location" 
where
cast(date_parse(event_date,'%Y%m%d') as date) = current_date - interval '1' day
