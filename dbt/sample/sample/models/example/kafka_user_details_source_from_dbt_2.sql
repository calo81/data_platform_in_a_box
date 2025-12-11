{{ config(materialized='source') }}
CREATE SOURCE IF NOT EXISTS {{ this }} (
  user_id int,
  first_name text,
  last_name text,
  ts text,
)
WITH (
  connector = 'kafka',
  topic = 'kafka_user_details',
  properties.bootstrap.server = 'kafka-platform:9092',
  properties.sasl.mechanism='PLAIN',
  properties.security.protocol='SASL_PLAINTEXT',
  properties.sasl.username='user1',
  properties.sasl.password='user1'
) FORMAT PLAIN ENCODE JSON;