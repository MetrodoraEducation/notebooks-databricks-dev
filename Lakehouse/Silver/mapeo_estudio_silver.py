# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

mapeo_estudio_df = spark.read.parquet(f"{bronze_folder_path}/lakehouse/mapeo_estudio/{current_date}")

# COMMAND ----------

mapeo_estudio_df.createOrReplaceTempView("mapeo_estudio_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.mapeo_estudio
# MAGIC USING mapeo_estudio_view 
# MAGIC ON silver_lakehouse.mapeo_estudio.estudio = mapeo_estudio.estudio
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
