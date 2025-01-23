# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW mapeo_estudio_view
# MAGIC     AS SELECT * FROM silver_lakehouse.mapeo_estudio

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.mapeo_estudio
# MAGIC USING mapeo_estudio_view 
# MAGIC ON gold_lakehouse.mapeo_estudio.estudio = mapeo_estudio_view.estudio
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
