# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingesting data

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

v_file_date

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType

# COMMAND ----------

#Setting Schema

races_schema = \
StructType (
    fields = 
    [
        StructField("raceid", IntegerType(),False),
        StructField("year", IntegerType(),True),
        StructField("round", IntegerType(),True),
        StructField("circuitid", IntegerType(),True),
        StructField("name", StringType(),True),
        StructField("date", DateType() ,True),
        StructField("time", StringType(),True),
        StructField("url", StringType(),True)
    ]
)

# COMMAND ----------

#Reading initial DF

races_df = spark.read \
.option("header", "true") \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")


# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, concat, lit

# COMMAND ----------

#Adding race timestamp
races_df_timestamp_added = races_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

#Select relevant columns and renaming - dropped URL
races_selected_df = races_df_timestamp_added.select(
    col("raceId").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    col("race_timestamp")
    # ,col("date"),
    # col("time")
    ).withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))




# COMMAND ----------

#adding ingestion date

races_final_df = add_ingestion_date(races_selected_df)



# COMMAND ----------

# MAGIC %md
# MAGIC ###Writing to datalake as Delta Table

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")