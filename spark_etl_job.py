# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_date, collect_list, map_from_arrays
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
import sys

# COMMAND ----------

spark

# COMMAND ----------

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("User Event ETL") \
    .getOrCreate()




# COMMAND ----------

# Define the file path
file_path = "/FileStore/tables/data.txt"

# Read data from the file into a DataFrame
df = spark.read.csv(file_path,inferSchema=True)

# Show the DataFrame
df.show(truncate=False)


# COMMAND ----------

# Rename columns
df = df.withColumnRenamed("_c0","id") \
               .withColumnRenamed("_c1","name") \
               .withColumnRenamed("_c2","value") \
               .withColumnRenamed("_c3","timestamp")

# Show the DataFrame with new column names
df.show(truncate=False)


# COMMAND ----------

df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

# Check for non-null values in each column
print("Null value check:")
df.select([F.count(F.when(F.col(c).isNotNull(), 1)).alias(c + "_non_nulls") for c in df.columns]).show()

# COMMAND ----------

# Count distinct values per column
print("Distinct values per column:")
df.select([F.countDistinct(F.col(c)).alias(f"{c}_distinct") for c in df.columns]).show()

# COMMAND ----------

# Count of records per 'name' field
print("Count per name:")
df.groupBy("name").count().show()

# COMMAND ----------

# Window specification to get latest timestamp per (id, name)
window_spec = Window.partitionBy("id", "name").orderBy(col("timestamp").desc())

# COMMAND ----------



# For each group (id, name), assign a row number starting from 1 based on latest timestamp
df_with_rank = df.withColumn("row_num", row_number().over(window_spec))

# Filter only the first row (row_num = 1), which is the most recent entry per (id, name)
df_latest = df_with_rank.filter(col("row_num") == 1)

# Drop the temporary 'row_num' and 'timestamp' columns since we don't need them anymore
df_latest = df_latest.drop("row_num", "timestamp")

# Add partition column (etl_date)
df_latest = df_latest.withColumn("etl_date", current_date())

# Display the result
df_latest.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Explanation of spark_etl_job.py:
# MAGIC
# MAGIC Step 1: We define a Window Spec to group the data by id and name, and sort by timestamp in descending order.
# MAGIC
# MAGIC Step 2: Use the row_number() function to assign a row number to each record within each group.
# MAGIC
# MAGIC Step 3: Filter out only the row with the most recent timestamp (row number 1).
# MAGIC
# MAGIC Step 4: We use map_from_entries to transform the (name, value) pairs into a MapType for each id.
# MAGIC
# MAGIC Step 5: The resulting DataFrame is partitioned by etl_date (current date) and written to Parquet format.

# COMMAND ----------

from pyspark.sql.functions import collect_list, map_from_arrays

# Group by 'id' and aggregate 'name' and 'value' into a Map
df_result = df_latest.groupBy("id", "etl_date").agg(
    map_from_arrays(
        collect_list("name"),
        collect_list("value")
    ).alias("settings_map")
)

# Show the final result
df_result.show(truncate=False)



# COMMAND ----------

# Write final result to Parquet partitioned by etl_date
final_output_path = "dbfs:/Users/kanneganti.likhitha@gmail.com/pyspark"

df_result.write \
    .mode("overwrite") \
    .partitionBy("etl_date") \
    .parquet(final_output_path)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/Users/kanneganti.likhitha@gmail.com/pyspark"))


# COMMAND ----------

# Copy from DBFS to /dbfs (local access for file API)
dbutils.fs.cp("dbfs:/Users/kanneganti.likhitha@gmail.com/pyspark", 
              "file:/tmp/pyspark_output", 
              recurse=True)


# COMMAND ----------

from pyspark.sql import functions as F

df_flattened = df_result.withColumn("settings_map", F.to_json("settings_map"))
df_flattened.show(truncate=False)

# COMMAND ----------

output_path = "/FileStore/tables/pyspark_txt"


df_flattened.write \
    .mode("overwrite") \
    .partitionBy("etl_date") \
    .option("header", True) \
    .option("delimiter", "\t") \
    .csv(output_path)


# COMMAND ----------

# Define the file path
file_path = "/FileStore/tables/pyspark_txt"

# Read the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Show the DataFrame to verify the data
df.show(truncate=False)


# COMMAND ----------

df_flattened.show(truncate=False)

# COMMAND ----------

# Verify the output
dbutils.fs.ls(output_path)