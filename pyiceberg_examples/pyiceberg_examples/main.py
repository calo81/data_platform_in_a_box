from pyiceberg.catalog.rest import RestCatalog
import pyarrow.parquet as pq
import os

warehouse_path = "/tmp/warehouse"
catalog = RestCatalog(
    "default",
    **{
        "uri": f"http://localhost:19120/iceberg",
        "warehouse": "warehouse1",
        "s3.access-key-id": "awsAccessKeyId",
        "s3.secret-access-key": "awsSecretAccessKey",
    },
)

catalog.create_namespace_if_not_exists("first_namespace")
df = pq.read_table(f"{os.path.dirname(os.path.abspath(__file__))}/../data_samples/taxi_data.parquet")

table = catalog.create_table_if_not_exists(
    "first_namespace.taxi_dataset",
    schema=df.schema,
)

table.overwrite(df)
print(len(table.scan().to_arrow()))
