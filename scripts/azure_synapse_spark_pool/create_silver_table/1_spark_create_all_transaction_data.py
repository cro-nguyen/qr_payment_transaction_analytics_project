# Azure Synapse Spark notebook source

# Cell 1
# MAGIC %md
# MAGIC # VNPAY QR Payment Analytics - Transaction Data Processing
# MAGIC ## 1_spark_create_all_transaction_data
# MAGIC 
# MAGIC This notebook processes 87M+ transaction records using Azure Synapse Spark Pool, implementing time-based partitioning for optimal query performance and creating external tables for downstream analytics.

# Cell 2
#Set the folder paths so that it can be used later. 
bronze_folder_path = 'abfss://bronze@vnpayproject.dfs.core.windows.net/'
silver_folder_path = 'abfss://silver@vnpayproject.dfs.core.windows.net/__unitystorage/schemas/72c947f6-1c6b-4578-9cc6-f65e3f3118a7/tables/'
new_silver_folder_path = 'abfss://silver@vnpayproject.dfs.core.windows.net/'
gold_folder_path = 'abfss://gold@vnpayproject.dfs.core.windows.net/'

# Cell 3
#Set the spark config to be able to get the partitioned columns year and month as strings rather than integers
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

# Cell 4
# Read with schema enforcement
all_transaction_df = spark.read.format("delta") \
    .option("mergeSchema", "false") \
    .load(f"{silver_folder_path}/3ef73633-4816-421f-aabf-76ee36b90d6a")

# Cell 5
display(all_transaction_df.limit(20))

# Cell 6
from pyspark.sql.functions import col, year, month, date_format

# Now create the columns
all_transaction_df = all_transaction_df \
    .withColumn("year", year(col("thoi_gian_thanh_toan"))) \
    .withColumn("month", date_format(col("thoi_gian_thanh_toan"), "MM"))

# Cell 7
display(all_transaction_df.limit(20))

# Cell 8
# Write to Delta Lake with partition by year and month
all_transaction_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save(f"{new_silver_folder_path}/alltransaction_fact")

# Cell 9
all_transaction_df_new = spark.read.format("delta") \
    .option("mergeSchema", "false") \
    .load(f"{new_silver_folder_path}/alltransaction_fact")

display(all_transaction_df_new.limit(20))

# Cell 10
all_transaction_df_new.count()

# Cell 11
# MAGIC %sql
# MAGIC 
# MAGIC -- Create database to which we are going to write the data
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS vnpay_silver_spark
# MAGIC LOCATION 'abfss://silver@vnpayproject.dfs.core.windows.net/';

# Cell 12
# Read the Delta Lake file into DataFrame
all_transaction_df_new = spark.read.format("delta") \
    .option("mergeSchema", "false") \
    .load(f"{new_silver_folder_path}/alltransaction_fact")

# Create External Table pointing to existing Delta Lake location
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS vnpay_silver_spark.alltransaction_fact
    USING DELTA
    LOCATION '{new_silver_folder_path}/alltransaction_fact'
""")

# Cell 13
# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook successfully:
# MAGIC - Processed 87,616,095 transaction records
# MAGIC - Added year/month partition columns for performance optimization
# MAGIC - Wrote data to Delta Lake with partitioning strategy
# MAGIC - Created external table for SQL Serverless Pool integration
# MAGIC - Enabled PowerBI connectivity through optimized data structure
