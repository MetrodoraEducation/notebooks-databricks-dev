# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tipo_negocio_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT tipo_negocio_desc AS tipo_negocio_desc,
# MAGIC     cod_tipo_negocio	AS cod_tipo_negocio
# MAGIC
# MAGIC FROM 
# MAGIC     gold_lakehouse.dim_estudio

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_negocio
# MAGIC USING tipo_negocio_view 
# MAGIC ON gold_lakehouse.dim_tipo_negocio.tipo_negocio_desc = tipo_negocio_view.tipo_negocio_desc
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_tipo_negocio.cod_tipo_negocio = tipo_negocio_view.cod_tipo_negocio
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_tipo_negocio.tipo_negocio_desc,gold_lakehouse.dim_tipo_negocio.cod_tipo_negocio)
# MAGIC VALUES (tipo_negocio_view.tipo_negocio_desc,tipo_negocio_view.cod_tipo_negocio)
