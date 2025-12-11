# data_platform_in_a_box
A sample K8s deployment of an open data platform

- Minio
- Trino
- Iceberg
- Lakekeeper
- Kafka
- Spark
- RisingWave
- DuckDB


## Locally

You need a locally running Kubernetes "cluster". Minikube will do just fine.
Create a namespace `data` in your kubernetes cluster.

Then:

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami 
helm dependency build
helm install data-platform -n data .
```


### Port forward Trino, Lakekeeper and Minio and RisingWave

```bash
k port-forward svc/data-platform-minio 9000:9000 -n data
k port-forward svc/data-platform-minio-console 9000:9000 -n data
k port-forward svc/data-platform-trino 8083:8080 -n data 
k port-forward svc/data-platform-risingwave 4567:4567 -n data  
k port-forward svc/data-platform-risingwave-meta-headless 5691:5691 -n data  
k port-forward svc/data-platform-lakekeeper 8181:8181
```

**NOTE**: You might need to create the `risingwave` postgres table by hand 

```
psql -h localhost -p 5432  -U postgres

# Then type password "hola"

create database risingwave;

\q
```

### Modify your /etc/hosts adding:

```
127.0.0.1       data-platform-minio
127.0.0.1       data-platform-lakekeeper
```



### Setup minio command line local client:

```bash
mc alias set local http://localhost:9000 awsAccessKeyId awsSecretAccessKey 
mc mb local/risingwave 
mc mb local/warehouse3
```

### Bootsrap Lakekeeper

```
curl -X POST http://127.0.0.1:8181/management/v1/bootstrap -H 'Content-Type: application/json' -d '{"accept-terms-of-use": true}'

curl -X POST http://127.0.0.1:8181/management/v1/warehouse \
  -H 'Content-Type: application/json' \
  -d '{
    "warehouse-name": "warehouse3",
    "delete-profile": { "type": "hard" },
    "storage-credential": {
      "type": "s3",
      "credential-type": "access-key",
      "aws-access-key-id": "awsAccessKeyId",
      "aws-secret-access-key": "awsSecretAccessKey"
    },
    "storage-profile": {
      "type": "s3",
      "bucket": "warehouse3",
      "region": "eu-west-1",
      "flavor": "s3-compat",
      "endpoint": "http://data-platform-minio:9000/",
      "path-style-access": true,
      "sts-enabled": false,
      "key-prefix": "warehouse3"
    }
  }'

```


### Run Minio console client locally (or just acces the one port forwarded above)

```bash
go install -v  github.com/minio/console/cmd/console@latest 
cd ~/go/bin
export CONSOLE_MINIO_SERVER=http://localhost:9000
./console server
```
Then login with:

```
username: awsAccessKeyId
password: awsSecretAccessKey
```

### Let's use duckdb to create some Iceberg tables

```
curl https://install.duckdb.org | sh

duckdb -ui
```

Then in the notebook opened:

```sql

INSTALL ICEBERG; LOAD ICEBERG;

DETACH lakekeeper_db;
ATTACH 'warehouse3' AS lakekeeper_db (
    TYPE iceberg,
    AUTHORIZATION_TYPE None,
    ENDPOINT 'http://data-platform-lakekeeper:8181/catalog'
);

SET s3_access_key_id='awsAccessKeyId';
SET s3_secret_access_key='awsSecretAccessKey';
SET s3_endpoint='http://data-platform-minio:9000';

create schema public;

create table lakekeeper_db.public.user_state_snapshot_2(user_id int, event_ts string, first_name string, last_name string, dob string, pay string);

select * from lakekeeper_db.public.user_state_snapshot_2

```



### You can also Use PyIceberg to populate some table:

Look in the file [main.py](pyiceberg_examples/pyiceberg_examples/main_all_k8s.py)

Execute it



### Using Rising Wave. First to select from the Iceberg table:

In `DBeaver` using a postgres (or actually RisingWave option is there) driver you can do:

Create the connection:

Forward the `RisingWave` `data-platform-risingwave` port `4567` to your localhost

Then connect from `DBeaver` or similar

```bash
jdbc:postgresql://localhost:4567/dev
```

use `root` and `root` for username and password

```sql
create source taxi_demo_source
with (
    connector = 'iceberg',
    s3.endpoint = 'http://data-platform-minio:9000',
    s3.access.key = 'awsAccessKeyId',
    s3.secret.key = 'awsSecretAccessKey',
    s3.region = 'eu-west-3',
    catalog.type = 'rest',
    catalog.name = '8deaa192-d45a-11f0-a03f-4b63baf89cb5',
    catalog.uri = 'http://data-platform-lakekeeper:8181/catalog',
    warehouse.path = 'warehouse3',
    database.name = 'public',
    table.name = 'taxi_dataset',
);


select * from taxi_demo_source;

```
You can also create tables from Risingwave

```sql
CREATE CONNECTION lakekeeper_catalog_conn2
WITH (
  type = 'iceberg',
  catalog.type = 'rest',
  catalog.uri = 'http://data-platform-lakekeeper:8181/catalog', -- URI of your Lakekeeper service
  warehouse.path = 'warehouse3',
  s3.endpoint = 'http://data-platform-minio:9000',
  s3.access.key = 'awsAccessKeyId',
  s3.secret.key = 'awsSecretAccessKey',
  s3.region = 'europe-west-3',
  s3.path.style.access = 'true'
);

SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn2';
ALTER SYSTEM SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn2';

CREATE TABLE users2 (
   user_id INT,
   user_name VARCHAR
) WITH (
  commit_checkpoint_interval = 1
)
engine = iceberg;

insert into users2 values (1, 'carlo');

select * from users2;

```

### Connect RisingWave to Kafka:


```sql
CREATE SOURCE kafka_user_details_source (
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


CREATE SOURCE kafka_user_pay_1_source (
  user_id int,
  pay_1 text,
  ts text,
)
WITH (
  connector = 'kafka',
  topic = 'kafka_user_pay_1',
  properties.bootstrap.server = 'kafka-platform:9092',
  properties.sasl.mechanism='PLAIN',
  properties.security.protocol='SASL_PLAINTEXT',
  properties.sasl.username='user1',
  properties.sasl.password='user1'
) FORMAT PLAIN ENCODE JSON;


CREATE SOURCE kafka_user_pay_2_source (
  user_id int,
  pay_2 text,
  ts text,
)
WITH (
  connector = 'kafka',
  topic = 'kafka_user_pay_2',
  properties.bootstrap.server = 'kafka-platform:9092',
  properties.sasl.mechanism='PLAIN',
  properties.security.protocol='SASL_PLAINTEXT',
  properties.sasl.username='user1',
  properties.sasl.password='user1'
) FORMAT PLAIN ENCODE JSON;


CREATE SOURCE kafka_user_extra_details_source (
  user_id int,
  dob text,
  ts text,
)
WITH (
  connector = 'kafka',
  topic = 'kafka_user_extra_details',
  properties.bootstrap.server = 'kafka-platform:9092',
  properties.sasl.mechanism='PLAIN',
  properties.security.protocol='SASL_PLAINTEXT',
  properties.sasl.username='user1',
  properties.sasl.password='user1'
) FORMAT PLAIN ENCODE JSON;
```

### Producing some Kafka messages:

Exec into the `kafka-client-deployment-xxxx-xx` pod. And from there execute

```bash
kafka-console-producer.sh  --producer.config /tmp/kafka-client.properties --bootstrap-server kafka-platform-broker-0.kafka-platform-broker-headless.data.svc.cluster.local:9092 --topic  kafka_user_details
# send the messages:

>{"user_id":1, "first_name":"carl", "last_name":"scar", "ts":"2025-01-12T10:15:30Z"}
```

```bash
kafka-console-producer.sh  --producer.config /tmp/kafka-client.properties --bootstrap-server kafka-platform-broker-0.kafka-platform-broker-headless.data.svc.cluster.local:9092 --topic  kafka_user_extra_details
# send the messages:

>{"user_id":1, "dob":"20000512", "ts":"2025-01-12T10:16:30Z"}
```

```bash
kafka-console-producer.sh  --producer.config /tmp/kafka-client.properties --bootstrap-server kafka-platform-broker-0.kafka-platform-broker-headless.data.svc.cluster.local:9092 --topic  kafka_user_pay_1
# send the messages:

>{"user_id":1, "pay_1":"100", "ts":"2025-01-12T10:17:30Z"}
```

```bash
kafka-console-producer.sh  --producer.config /tmp/kafka-client.properties --bootstrap-server kafka-platform-broker-0.kafka-platform-broker-headless.data.svc.cluster.local:9092 --topic  kafka_user_pay_2
# send the messages:

>{"user_id":1, "pay_2":"300", "ts":"2025-01-12T10:18:30Z"}
```

### Let's do some transformations, joins and log of changes

#### First a join across them producing the joined reccord


```sql

CREATE MATERIALIZED VIEW user_details_and_extra_mv AS
SELECT
    COALESCE(u.user_id, e.user_id) AS user_id,
    u.first_name,
    u.last_name,
    e.dob,
    -- Use the latest timestamp from either source
    GREATEST(u.ts, e.ts) AS latest_ts
FROM
    kafka_user_details_source AS u
FULL OUTER JOIN
    kafka_user_extra_details_source AS e
ON
    u.user_id = e.user_id;



CREATE MATERIALIZED VIEW user_pay_combined_mv AS
-- Select the combined data from the Full Outer Join
SELECT
    COALESCE(p1.user_id, p2.user_id) AS user_id,
    
    -- Determine the 'pay' column using the latest timestamp (Last-Write-Wins)
    CASE
        -- If both payments exist, choose the one with the later timestamp
        WHEN p1.ts IS NOT NULL AND p2.ts IS NOT NULL THEN
            CASE
                WHEN p1.ts >= p2.ts THEN p1.pay_1
                ELSE p2.pay_2
            END
        -- If only pay_1 exists (p2 is NULL), use pay_1
        WHEN p1.ts IS NOT NULL THEN p1.pay_1
        -- If only pay_2 exists (p1 is NULL), use pay_2
        WHEN p2.ts IS NOT NULL THEN p2.pay_2
        -- Otherwise (both are NULL, which shouldn't happen here but for completeness)
        ELSE NULL
    END AS pay,
    
    -- Record the latest timestamp used
    GREATEST(p1.ts, p2.ts) AS latest_ts
FROM
    kafka_user_pay_1_source AS p1
FULL OUTER JOIN
    kafka_user_pay_2_source AS p2
ON
    p1.user_id = p2.user_id;
    
    
CREATE MATERIALIZED VIEW unified_user_profile_mv AS
SELECT
    COALESCE(d.user_id, p.user_id) AS user_id,
    d.first_name,
    d.last_name,
    d.dob,
    p.pay, -- The single, latest payment value
    -- The final timestamp is the latest across all streams
    GREATEST(d.latest_ts, p.latest_ts) AS last_updated_ts
FROM
    user_details_and_extra_mv AS d
FULL OUTER JOIN
    user_pay_combined_mv AS p
ON
    d.user_id = p.user_id;

select * from unified_user_profile_mv;
```

#### Then also keeping them with UNION as log of changes

```sql
CREATE MATERIALIZED VIEW log_user_details_mv AS
SELECT
    user_id,
    first_name,
    last_name,
    NULL AS dob, -- Not provided in this stream
    NULL AS pay,   -- Not provided in this stream
    ts AS event_ts
FROM
    kafka_user_details_source;


CREATE MATERIALIZED VIEW log_user_extra_details_mv AS
SELECT
    user_id,
    NULL AS first_name, -- Not provided in this stream
    NULL AS last_name,  -- Not provided in this stream
    dob,
    NULL AS pay,
    ts AS event_ts
FROM
    kafka_user_extra_details_source;

CREATE MATERIALIZED VIEW log_user_pay_1_mv AS
SELECT
    user_id,
    NULL AS first_name,
    NULL AS last_name,
    NULL AS dob,
    pay_1 AS pay, -- Renamed pay_1 to pay
    ts AS event_ts
FROM
    kafka_user_pay_1_source;

CREATE MATERIALIZED VIEW log_user_pay_2_mv AS
SELECT
    user_id,
    NULL AS first_name,
    NULL AS last_name,
    NULL AS dob,
    pay_2 AS pay, -- Renamed pay_2 to pay
    ts AS event_ts
FROM
    kafka_user_pay_2_source;

CREATE MATERIALIZED VIEW user_change_history_mv AS
SELECT * FROM log_user_details_mv
UNION ALL
SELECT * FROM log_user_extra_details_mv
UNION ALL
SELECT * FROM log_user_pay_1_mv
UNION ALL
SELECT * FROM log_user_pay_2_mv
ORDER BY user_id, event_ts;


select * from user_change_history_mv;


CREATE MATERIALIZED VIEW user_state_snapshot_mv AS
SELECT
    user_id,
    event_ts, -- The timestamp of the event that triggered this record
    
    -- CORRECTED SYNTAX: IGNORE NULLS is inside the function call
    LAST_VALUE(first_name IGNORE NULLS) OVER user_partition AS first_name,
    LAST_VALUE(last_name IGNORE NULLS) OVER user_partition AS last_name,
    LAST_VALUE(dob IGNORE NULLS) OVER user_partition AS dob,
    LAST_VALUE(pay IGNORE NULLS) OVER user_partition AS pay
FROM
    user_change_history_mv
WINDOW user_partition AS (
    PARTITION BY user_id
    ORDER BY event_ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

select * from user_state_snapshot_mv;
```

### Sink the resulting Data to Iceberg:

```sql
create SINK kafka_to_iceberg_sink
AS 
SELECT 
    *
FROM user_state_snapshot_mv 
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
    table.name = 'user_state_snapshot_2'
);
```

### Back in DUCKDB UI, qquery the data:

```sql
select * from lakekeeper_db.public.user_state_snapshot_2;

select * from  iceberg_metadata('lakekeeper_db.public.user_state_snapshot_2');

SELECT *
FROM duckdb_functions()
WHERE function_name LIKE 'iceberg%';

```

### RisingWave Dashboard.

Forward port `5691` of service `data-platform-risingwave-meta-headless` locally

go to [http://localhost:5691/](http://localhost:5691/)


### Using DBT

Go to folder `dbt/sample/sample`

Make sure you have a `python 3.11` available.

then `pip install uv`

then `uv sync`

#### Create the last table of the sync in duckdb iceberg

```sql
create table lakekeeper_db.public.user_state_snapshot_3
(user_id int, 
event_ts string, 
first_name string, 
last_name string, 
dob string, 
pay string);
```

then back in the dbt project `dbt run --profiles-dir .`

you can now query the last table in duckdb `select * from lakekeeper_db.public.user_state_snapshot_3` or query the materialized view from *dbeaver* (the rising wave connection) `select * from materialized_view_dbt;`


### Playing with Trino and iceberg:

```sql

create table first_namespace.cosas (
 k int,
  v1 varchar,
  v2 varchar
);

insert into first_namespace.cosas (k, v1, v2) values (
  1, 'hola', 'chao'
);

insert into first_namespace.cosas (k, v1, v2) values (
  2, 'hi', 'bye'
);
    
select * from first_namespace.cosas;

# Time traveling: 

# commit antes de actual

select * from first_namespace."cosas#~2";
    

# Results at milliseconds since the epoch

select * from first_namespace."cosas#*1732494659719";
    
select * from first_namespace."cosas$manifests";
    
select * from first_namespace."cosas$files";
    

select * from first_namespace."cosas$snapshots";





```


### Some Postgres to Iceberg Replication

Port forward port `5432` of service `data-platform-postgresql`

```sql
-- PostgreSQL table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO orders (customer_id, order_status, total_amount) VALUES
    (101, 'pending', 299.99),
    (102, 'processing', 1250.50),
    (101, 'shipped', 89.99),
    (103, 'delivered', 499.99),
    (102, 'cancelled', 750.00);

```

In Trino let's create the iceberg table:

```sql

create table first_namespace.from_postgres3(order_status varchar,
    order_count BIGINT,
    total_revenue DECIMAL)
```

Then back in RisingWave to create source and sink:

```sql
CREATE table pg_cdc_source2(
    order_id INTEGER,
    customer_id INTEGER,
    order_status VARCHAR,
    total_amount DECIMAL,
    primary key(order_id)
) WITH (
    connector = 'postgres-cdc',
    hostname = 'data-platform-postgresql',
    port = '5432',
    username = 'postgres',
    password = 'hola',
    database.name = 'postgres',
    table.name = 'orders'
);
    
    
CREATE SINK orders_status_summary AS
SELECT
    order_status as order_status,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue
FROM pg_cdc_source2
GROUP BY order_status
WITH (connector = 'iceberg',
    type = 'append-only',
    force_append_only='true',
    s3.endpoint = 'http://data-platform-minio:9000',
    s3.access.key = 'awsAccessKeyId',
    s3.secret.key = 'awsSecretAccessKey',
    s3.region = 'us-east-1',
    catalog.type = 'rest',
    catalog.name = 'main',
    catalog.uri = 'http://nessie-proxy-service:19120/iceberg',
    warehouse.path = 's3://warehouse1/',
    database.name = 'first_namespace',
    table.name = 'from_postgres3')


```

### Using From BigQuery

I will assume a bucket in gcs named `iceberg_tables`.

A configuration is provided to use Iceberg with GCS storage instead of Minio. Also a Trino configuration to be able to query it from Trino.

Modify the secret [gcs-service-account-secret.yaml](charts/data-platform/templates/gcs-service-account-secret.yaml) with the Service Account Json that you want that is able to create objects in the bucket in GCS

You should have a user that also can write into that bucket. In order to populate it with some stuff from Python.

Locally do `gcloud auth application-default login` and login with that user

Then you can run the file [main_gcs_try.py](pyiceberg_examples/pyiceberg_examples/main_gcs_try.py) which will populate an Iceberg table in GCS.

To query this from BigQuery:

In BigQuery First create an [external connection](https://cloud.google.com/bigquery/docs/create-cloud-resource-connection)

I'll assume next the connection is named `projects/playground-testing-364317/locations/europe-west2/connections/iceberg_meta_2`

In your BigQuery Console do the following (replacing paths as needed)

```sql
 CREATE EXTERNAL TABLE `iceberg_stuff2.taxi3`
  WITH CONNECTION `projects/playground-testing-364317/locations/europe-west2/connections/iceberg_meta_2`
  OPTIONS (
         format = 'ICEBERG',
         uris = ["gs://iceberg_tables/first_namespace_gcs/taxi_dataset_6340ccc3-890c-4875-afd1-798199c60e70/metadata/00000-60c2a882-e50e-4957-8f61-0486d9e46895.metadata.json"]
   )
```

You can now query that table in BigQuery:

```sql
SELECT * FROM `playground-testing-364317.iceberg_stuff2.taxi3` LIMIT 1000
```

**NOTE**: The query part from BigQuery as shown is not extremely great as the metadata file needs to be changed by hand every time. However using the GCS for storing the data and metadata is perfectly fine.

To query the same table from Trino. Again connect to Trino and port forward as we did  before.

But now select the "schema" or "catalog" `iceberg_gcs` and the database `first_namespace_gcs`

You can now query from Trino:

```sql
select * from "taxi_dataset";
```

### Run Trino Locally

You can query you Iceberg data lakehouse with SQL now. In particular we can use trino that has been deployed here.

Open your favorite JDBC client (I am using DBeaver)

Then, install the Trino JDBC driver and configure a new connection like:

Connection URL:

```
jdbc:trino://localhost:8083/iceberg
```

username: xxx

no password

you can now do a select like:

```sql
SELECT *
FROM first_namespace.taxi_dataset;
```