{{ config(materialized='sink') }}
create SINK {{ this }}
AS 
SELECT 
    *
FROM {{ref('materialized_view_dbt')}} 
with (
    connector = 'iceberg',
    type = 'append-only',
    force_append_only='true',
    s3.endpoint = 'http://data-platform-minio:9000',
    s3.access.key = 'awsAccessKeyId',
    s3.secret.key = 'awsSecretAccessKey',
    s3.region = 'eu-west-3',
    catalog.type = 'rest',
    catalog.uri = 'http://data-platform-lakekeeper:8181/catalog',
    warehouse.path = 'warehouse3',
    database.name = 'public',
    table.name = 'user_state_snapshot_3'
);