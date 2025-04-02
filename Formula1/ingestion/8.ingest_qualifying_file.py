# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Read Qualifying Folder - JSON

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

v_file_date

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(
    fields = [
        StructField("qualifyId", IntegerType(), False),
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), False),
        StructField("constructorId", IntegerType(), False),
        StructField("number", IntegerType(), False),
        StructField("position", IntegerType(), False),
        StructField("q1", StringType(), True),
        StructField("q2", StringType(), True),
        StructField("q3", StringType(), True),
    ]
)

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split_*.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2- Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

ingestion_date_added_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

final_df = ingestion_date_added_df.withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write output incrementally as a Delta Table

# COMMAND ----------

# incremental_load(final_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

#Merge condition is on composite primary key
merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"

merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")