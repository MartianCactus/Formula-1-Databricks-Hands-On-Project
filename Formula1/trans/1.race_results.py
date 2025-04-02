# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run  "../includes/configuration"

# COMMAND ----------

# MAGIC %run  "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")
results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date")

# display(races_df)
# display(circuits_df)
# display(drivers_df)
# display(constructors_df)
# display(results_df)

# COMMAND ----------

presentation_df = results_df.join(races_df, races_df.race_id == results_df.result_race_id, "left") \
                            .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "left") \
                            .join(drivers_df, results_df.driver_Id == drivers_df.driver_id, "left") \
                            .join(constructors_df, results_df.constructor_Id == constructors_df.constructor_id, "left")\
                            .select( \
                                races_df.race_id,
                                races_df.race_year, 
                                races_df.name.alias("race_name"),
                                races_df.race_timestamp.alias("race_date"),
                                circuits_df.location.alias("circuit_location"),
                                drivers_df.name.alias("driver_name"),
                                drivers_df.number.alias("driver_number"),
                                drivers_df.nationality.alias("driver_nationality"),
                                constructors_df.name.alias("team"),
                                results_df.grid,
                                results_df.fastest_lap,
                                results_df.time.alias("race_time"),
                                results_df.points,
                                results_df.position,
                                results_df.result_file_date
                                ).withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

presentation_df = add_ingestion_date(presentation_df)

# COMMAND ----------

# incremental_load(presentation_df, "f1_presentation", "race_results", "race_id")

# COMMAND ----------

#Merge condition is on composite primary key
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name"

merge_delta_data(presentation_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC   FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;