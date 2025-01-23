# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tipo_formacion_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT tipo_formacion_desc AS tipo_formacion_desc,
# MAGIC     cod_tipo_formacion	AS cod_tipo_formacion
# MAGIC
# MAGIC FROM 
# MAGIC     gold_lakehouse.dim_estudio

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_formacion
# MAGIC USING tipo_formacion_view 
# MAGIC ON gold_lakehouse.dim_tipo_formacion.tipo_formacion_desc = tipo_formacion_view.tipo_formacion_desc
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_tipo_formacion.cod_tipo_formacion = tipo_formacion_view.cod_tipo_formacion
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_tipo_formacion.tipo_formacion_desc,gold_lakehouse.dim_tipo_formacion.cod_tipo_formacion)
# MAGIC VALUES (tipo_formacion_view.tipo_formacion_desc,tipo_formacion_view.cod_tipo_formacion)
