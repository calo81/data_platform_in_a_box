# data_platform_in_a_box
A sample K8s deployment of an open data platform

- Minio
- Trino
- Iceberg
- Nessie
- Kafka
- Spark
- RisingWave


## Locally

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami 
helm repo add nessie https://charts.projectnessie.org
helm dependency build
helm install data-platform -n data .
```

### Port forward Trino, Nessie and Minio and RisingWave

```bash
k port-forward svc/data-platform-minio 9000:9000 -n data
k port-forward svc/data-platform-nessie 19120:19120 -n data  
k port-forward svc/data-platform-trino 8083:8080 -n data 
k port-forward svc/data-platform-risingwave 4567:4567 -n data  
```

### Setup minio command line local client:

```bash
mc alias set local http://localhost:9000 awsAccessKeyId awsSecretAccessKey 
mc mb local/risingwave 
mc mb local/warehouse1
mc ls local/warehouse1/first_namespace/
```

### Run Minio console client locally

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

### Use PyIceberg to populate some table:

Look in the file [main.py](pyiceberg_examples/pyiceberg_examples/main.py)

Execute it

### Run Nessie Client locally

```bash
curl -L -o nessie-cli-0.100.0.jar https://github.com/projectnessie/nessie/releases/download/nessie-0.100.0/nessie-cli-0.100.0.jar 
java -jar nessie-cli-0.100.0.jar
```

Then:

```sql
CONNECT TO http://localhost:19120/iceberg
SHOW NAMESPACE first_namespace
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

### Using Rising Wave. First to select from the Iceberg table:

As there is an error in Rising Wave when crafting the URL to connect to Iceberg Rest in Nessie, I had to do a workaround and deploy a Proxy (you see it named `nessie-proxy-service` in the K8s)

Worst workaround is that now we have to by hand modify the `/etc/hosts` file in the `data-platform-risingwave-frontend` pod

so the workaround:

```bash
frontend=$(k get pods -n data -l 'risingwave/component=frontend' | awk  '{print $1}' | tail -n 1)
meta=$(k get pods -n data -l 'risingwave/component=meta' | awk  '{print $1}' | tail -n 1)
compute=$(k get pods -n data -l 'risingwave/component=compute' | awk  '{print $1}' | tail -n 1)

# Grab the name of the pod then
ip=$(k get service nessie-proxy-service -o jsonpath='{.spec.clusterIP}' -n data)

k exec -it $frontend -n data -- bash -c "echo '$ip  data-platform-nessie' > /etc/hosts"
k exec -it $frontend -n data -- bash -c "echo '$ip  data-platform-nessie.data.svc.cluster.local' >> /etc/hosts"
k exec -it $meta -n data -- bash -c "echo '$ip  data-platform-nessie' > /etc/hosts"
k exec -it $compute -n data -- bash -c "echo '$ip  data-platform-nessie' > /etc/hosts"
k exec -it $compute -n data -- bash -c "echo '$ip  data-platform-nessie.data.svc.cluster.local' >> /etc/hosts"
k exec -it $meta -n data -- bash -c "echo '$ip  data-platform-nessie.data.svc.cluster.local' >> /etc/hosts"

```

if Using `fish` shell then it is:

```bash
set frontend $(k get pods -n data -l 'risingwave/component=frontend' | awk  '{print $1}' | tail -n 1)                                                                                                               15:44:11
                         set meta $(k get pods -n data -l 'risingwave/component=meta' | awk  '{print $1}' | tail -n 1)
                         set compute $(k get pods -n data -l 'risingwave/component=compute' | awk  '{print $1}' | tail -n 1)

                         # Grab the name of the pod then
                         set ip $(k get service nessie-proxy-service -o jsonpath='{.spec.clusterIP}' -n data)

                         k exec -it $frontend -n data -- bash -c "echo '$ip  data-platform-nessie' > /etc/hosts"
                         k exec -it $frontend -n data -- bash -c "echo '$ip  data-platform-nessie.data.svc.cluster.local' >> /etc/hosts"
                         k exec -it $meta -n data -- bash -c "echo '$ip  data-platform-nessie' > /etc/hosts"
                         k exec -it $compute -n data -- bash -c "echo '$ip  data-platform-nessie' > /etc/hosts"
                         k exec -it $compute -n data -- bash -c "echo '$ip  data-platform-nessie.data.svc.cluster.local' >> /etc/hosts"
                         k exec -it $meta -n data -- bash -c "echo '$ip  data-platform-nessie.data.svc.cluster.local' >> /etc/hosts"
```

The previous workaround process needs to be done everytime the `risingwave` or the `nessie-proxy-service` restart


Then in `DBeaver` using a postgres (or actually RisingWave option is there) driver you can do:

Create the connection:

Forward the `RisingWave` `data-platform-risingwave` port `4567` to your localhost

Then connect from `DBeaver` or similar

```bash
jdbc:postgresql://localhost:4567/dev
```

use `root` and `root` for username and password

```sql
create source source_demo_rest
with (
    connector = 'iceberg',
    s3.endpoint = 'http://data-platform-minio:9000',
    s3.access.key = 'awsAccessKeyId',
    s3.secret.key = 'awsSecretAccessKey',
    s3.region = 'us-east-1',
    catalog.type = 'rest',
    catalog.name = 'main',
    catalog.uri = 'http://data-platform-nessie:19120/iceberg',
    warehouse.path = 's3://warehouse1/',
    database.name = 'first_namespace',
    table.name = 'taxi_dataset',
);


select * from source_demo_rest;
```

### Connect RisingWave to Kafka:


```sql
CREATE SOURCE kafka_source (
  k int,
  v1 text,
  v2 text
)
WITH (
  connector = 'kafka',
  topic = 'test_topic',
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
kafka-console-producer.sh  --producer.config /tmp/kafka-client.properties --broker-list kafka-platform-broker-0.kafka-platform-broker-headless.data.svc.cluster.local:9092 --topic  test_topic
# send the messages:

>{"k":1, "v1":"car", "v2":"scar"}
```

### From Kafka To Iceberg:

```sql

create source taxi_source4
with (
    connector = 'iceberg',
    s3.endpoint = 'http://data-platform-minio:9000',
    s3.access.key = 'awsAccessKeyId',
    s3.secret.key = 'awsSecretAccessKey',
    s3.region = 'us-east-1',
    catalog.type = 'rest',
    catalog.name = 'main',
    catalog.uri = 'http://data-platform-nessie:19120/iceberg',
    warehouse.path = 's3://warehouse1/',
    database.name = 'first_namespace',
    table.name = 'taxi_dataset_from_kafka_2',
);




select * from taxi_source4;


CREATE SOURCE kafka_source3 (
  k int,
  v1 text,
  v2 text
)
WITH (
  connector = 'kafka',
  topic = 'test_topic',
  properties.bootstrap.server = 'kafka-platform:9092',
  properties.sasl.mechanism='PLAIN',
  properties.security.protocol='SASL_PLAINTEXT',
  properties.sasl.username='user1',
  properties.sasl.password='user1'
) FORMAT PLAIN ENCODE JSON;

select * from kafka_source3;

CREATE SINK iceberg_sink AS 
SELECT 
    cast( k as STRING) as k,
    v1,
    v2
FROM kafka_source3 
with (
    connector = 'iceberg',
    type = 'append-only',
    s3.endpoint = 'http://data-platform-minio:9000',
    s3.access.key = 'awsAccessKeyId',
    s3.secret.key = 'awsSecretAccessKey',
    s3.region = 'us-east-1',
    catalog.type = 'rest',
    catalog.name = 'main',
    catalog.uri = 'http://nessie-proxy-service:19120/iceberg',
    warehouse.path = 's3://warehouse1/',
    database.name = 'first_namespace',
    table.name = 'taxi_dataset_from_kafka_2',
);


```

### RisingWave Dashboard.

Forward port `5691` of service `data-platform-risingwave-meta-headless` locally

go to [http://localhost:5691/](http://localhost:5691/)


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

# extras

### Using From BigQuery

```sql
 CREATE EXTERNAL TABLE `iceberg_stuff2.taxi`
  WITH CONNECTION `projects/playground-testing-364317/locations/europe-west2/connections/iceberg_meta_2`
  OPTIONS (
         format = 'ICEBERG',
         uris = ["gs://iceberg_tables/1.metadata.json"]
   )
```