import ray
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from ray.data.dataset import Dataset
# import ray.data.datasource.parquet_datasource as pq

# Initialize Ray
ray.init()

# Oracle database connection settings
jdbc_url = "oracle+cx_oracle://mockschema:mockschema@10.164.0.4:1521/ORCL19"
table_name = "EMPLOYEES_10M"
partition_column = "employee_id"

# Step 1: Fetch min and max values for the partition column
engine = create_engine(jdbc_url)
with engine.connect() as connection:
    result = connection.execute(f"SELECT MIN({partition_column}), MAX({partition_column}) FROM {table_name}")
    min_val, max_val = result.fetchone()

# Define the number of partitions for parallelism
num_partitions = 128
partition_size = (max_val - min_val) // num_partitions

# Function to fetch partitioned data from Oracle in parallel
def fetch_partition_data(lower_bound, upper_bound):
    query = f"SELECT * FROM {table_name} WHERE {partition_column} BETWEEN {lower_bound} AND {upper_bound}"
    df = pd.read_sql(query, con=engine)
    return df

# Create ranges for partitioning
ranges = [
    (min_val + i * partition_size, min_val + (i + 1) * partition_size - 1)
    for i in range(num_partitions)
]
ranges[-1] = (ranges[-1][0], max_val)  # Adjust last partition to include max_val

# Step 2: Parallel data loading
futures = [
    ray.remote(fetch_partition_data).remote(lower, upper)
    for lower, upper in ranges
]
dfs = ray.get(futures)

# Combine all partitions into a Ray Dataset
dataset = ray.data.from_pandas(dfs)

# Define the output path for writing the Parquet file
output_path = "gs://ora_ingest/parquet_files"

# Write dataset as Parquet
dataset.write_datasource(pq.ParquetDatasource(), path=output_path)

# Shut down Ray
ray.shutdown()
