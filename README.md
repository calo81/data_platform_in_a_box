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
k port-forward svc/data-platform-nessie 19120:19120 -n data

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