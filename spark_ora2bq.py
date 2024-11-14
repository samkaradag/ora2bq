from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("OracleToParquet") \
    .config("spark.jars", "gs://ora_ingest/libs/ojdbc8.jar") \
    .getOrCreate()

# Oracle database connection properties
jdbc_url = "jdbc:oracle:thin:@//10.164.0.4:1521/ORCL19"
db_properties = {
    "user": "mockschema",
    "password": "mockschema",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Table to read and partition column
table_name = "EMPLOYEES_10M"
partition_column = "employee_id"  # Column used for partitioning

# Step 1: Fetch min and max values of the partition column dynamically
min_max_query = f"(SELECT MIN({partition_column}) AS min_val, MAX({partition_column}) max_val FROM {table_name}) min_max_temp"
min_max_df = spark.read.jdbc(url=jdbc_url, table=min_max_query, properties=db_properties)

# Extract min and max values
min_val, max_val = min_max_df.select(F.col("min_val").cast("int"), F.col("max_val").cast("int")).first()

# Define number of partitions based on data size or cluster resources
num_partitions = 128 

# Step 2: Load data from Oracle using dynamic min and max values for partitioning
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .option("driver", db_properties["driver"]) \
    .option("partitionColumn", partition_column) \
    .option("lowerBound", min_val) \
    .option("upperBound", max_val) \
    .option("numPartitions", num_partitions) \
    .load()

# Specify the path to write the Parquet file
output_path = "gs://ora_ingest/parquet_files"

# Write DataFrame as Parquet
df.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()