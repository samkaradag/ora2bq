from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import argparse

def oracle_to_parquet(jdbc_url, table_name, partition_column, user, password, output_path, num_partitions=128):
    """
    Reads data from an Oracle table, partitions it, and writes it to Parquet format.

    Args:
        jdbc_url (str): JDBC connection string for the Oracle database.
        table_name (str): Name of the table to read from.
        partition_column (str): Column to use for partitioning.
        user (str): Username for the Oracle database.
        password (str): Password for the Oracle database.
        output_path (str): Path to write the Parquet files.
        num_partitions (int, optional): Number of partitions. Defaults to 128.
    """

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("OracleToParquet") \
        .config("spark.jars", "gs://ora_ingest/libs/ojdbc8.jar") \
        .getOrCreate()

    # Oracle database connection properties
    db_properties = {
        "user": user,
        "password": password,
        "driver": "oracle.jdbc.driver.OracleDriver"
    }

    # Step 1: Fetch min and max values of the partition column dynamically
    min_max_query = f"(SELECT MIN({partition_column}) AS min_val, MAX({partition_column}) max_val FROM {table_name}) min_max_temp"
    min_max_df = spark.read.jdbc(url=jdbc_url, table=min_max_query, properties=db_properties)

    # Extract min and max values
    min_val, max_val = min_max_df.select(F.col("min_val").cast("int"), F.col("max_val").cast("int")).first()

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

    # Write DataFrame as Parquet
    df.write.mode("overwrite").parquet(output_path)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read data from Oracle and write to Parquet.")
    parser.add_argument("--jdbc_url", required=True, help="JDBC connection string for Oracle")
    parser.add_argument("--table_name", required=True, help="Name of the Oracle table")
    parser.add_argument("--partition_column", required=True, help="Column to partition by")
    parser.add_argument("--user", required=True, help="Oracle database username")
    parser.add_argument("--password", required=True, help="Oracle database password")
    parser.add_argument("--output_path", required=True, help="Path to write Parquet files")
    parser.add_argument("--num_partitions", type=int, default=128, help="Number of partitions (default: 128)")

    args = parser.parse_args()

    oracle_to_parquet(args.jdbc_url, args.table_name, args.partition_column, args.user, 
                      args.password, args.output_path, args.num_partitions)