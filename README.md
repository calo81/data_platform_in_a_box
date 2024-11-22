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

### Port forward Trino, Nessie and Minio

```bash
k port-forward svc/data-platform-minio 9000:9000 -n data
mc mb local/warehouse1 
mc ls local/warehouse1/first_namespace/
```

### Setup minio command line local client:

```bash
mc alias set local https://localhost:9000 awsAccessKeyId awsSecretAccessKey 
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

The previous workaround process needs to be done everytime the `risingwave` or the `nessie-proxy-service` restart


Then in `DBeaver` using a postgres (or actually RisingWave option is there) driver you can do:

Create the connection:

Forward the `RisingWave` `data-platform-risingwave` port `4567` to your localhost

Then connect from `DBeaver` or similar

```bash
jdbc:postgresql://localhost:4567/dev
```

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
    catalog.uri = 'http://nessie-proxy-service:19120/iceberg',
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
    catalog.uri = 'http://nessie-proxy-service:19120/iceberg',
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