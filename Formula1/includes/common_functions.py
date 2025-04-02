# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def generate_column_list_for_incremental(df, target_col):
    
    # Getting schema
    schema = df.schema.names

    # Putting partitioning column (target) at the end of the list
    schema.remove(target_col)
    schema.append(target_col)

    #returning the final schema for the big boi incremental function
    return schema

# COMMAND ----------

def incremental_load(dataframe, database, table, partition):

    #Setting Spark configuration to dynamic
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    #Placing partitioning column at the end of the final dataframe before writing for the InsertInto method to work correctly
    schema = generate_column_list_for_incremental(dataframe, partition)
    dataframe = dataframe.select(schema)
    # print(dataframe.schema.names)
    # return dataframe

    if (spark._jsparkSession.catalog().tableExists(f"{database}.{table}")):
        dataframe.write.mode("overwrite").insertInto(f"{database}.{table}")
    else:
        dataframe.write.mode("overwrite").partitionBy(partition).format("parquet").saveAsTable(f"{database}.{table}")

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optiimzer.dynamicPartitionPruning", "true")
    
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition) \
                .whenMatchedUpdateAll()\
                .whenNotMatchedInsertAll()\
                .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")