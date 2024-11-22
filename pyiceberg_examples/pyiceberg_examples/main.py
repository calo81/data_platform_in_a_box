from pyiceberg.catalog.rest import RestCatalog
import pyarrow.parquet as pq
import os
import pandas as pd

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

data = {
    "k": ["1", "2", "3", "4", "5"],
    "v1": ["text1", "text2", "text3", "text4", "text5"],
    "v2": ["value1", "value2", "value3", "value4", "value5"]
}

df2 = pd.DataFrame(data)
df2.to_parquet('df2.parquet')

df2 = pq.read_table('df2.parquet')

table = catalog.create_table_if_not_exists(
    "first_namespace.taxi_dataset_from_kafka_2",
    schema=df2.schema,
)

table.overwrite(df2)


