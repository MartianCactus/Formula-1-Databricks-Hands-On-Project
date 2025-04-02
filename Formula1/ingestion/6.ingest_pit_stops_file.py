# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Read JSON File

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

pit_stops_schema = StructType(
    fields = [
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("stop", StringType(), True),
        StructField("lap", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("milliseconds", IntegerType(), True)
    ]
)

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2- Rename columns and add new columns

# COMMAND ----------

ingestion_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = ingestion_date_df.withColumnRenamed("driverId", "driver_Id") \
.withColumnRenamed("raceId", "race_Id")\
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Incrementally write delta table to processed container

# COMMAND ----------

# incremental_load(final_df, "f1_processed", "pit_stops", "race_Id")

# COMMAND ----------

#Merge condition is on composite primary key
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"

merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_Id')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_processed.pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")