{{ config(materialized='materialized_view') }}
SELECT
    user_id,
    first_name,
    last_name,
    NULL AS dob, -- Not provided in this stream
    NULL AS pay,   -- Not provided in this stream
    ts AS event_ts
FROM
    {{ ref('kafka_user_details_source_from_dbt_2') }};