# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Read Lap times folder - CSV files

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

lap_times_schema = StructType(
    fields = [
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("lap", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True)
    ]
)

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2- Rename columns and add new columns

# COMMAND ----------

ingestion_date_added_df = add_ingestion_date(lap_times_df) 

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = ingestion_date_added_df.withColumnRenamed("driverId", "driver_Id") \
.withColumnRenamed("raceId", "race_Id")\
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write output incrementally as a Delta Table 

# COMMAND ----------

# incremental_load(final_df, "f1_processed", "lap_times", "race_Id")

# COMMAND ----------

#Merge condition is on composite primary key
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"

merge_delta_data(final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_Id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_Id, COUNT(1)
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")