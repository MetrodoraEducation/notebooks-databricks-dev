# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW localidad_sales_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT localidad AS nombre_localidad
# MAGIC
# MAGIC FROM 
# MAGIC     silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_localidad
# MAGIC USING localidad_sales_view 
# MAGIC ON gold_lakehouse.dim_localidad.nombre_localidad = localidad_sales_view.nombre_localidad
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_localidad.nombre_localidad)
# MAGIC VALUES (localidad_sales_view.nombre_localidad)
