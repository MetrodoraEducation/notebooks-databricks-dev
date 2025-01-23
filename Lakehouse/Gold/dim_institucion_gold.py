# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW institucion_sales_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT 
# MAGIC     institucion AS nombre_institucion
# MAGIC FROM 
# MAGIC     silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_institucion
# MAGIC USING institucion_sales_view 
# MAGIC ON gold_lakehouse.dim_institucion.nombre_institucion = institucion_sales_view.nombre_institucion
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_institucion.nombre_institucion)
# MAGIC VALUES (institucion_sales_view.nombre_institucion)
