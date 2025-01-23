# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW origen_campania_sales_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT 
# MAGIC     origen_campania AS nombre_origen_campania, 
# MAGIC     (SELECT IFNULL(MAX(utm_type), 'n/a') FROM gold_lakehouse.mapeo_origen_campania WHERE utm_source = origen_campania) AS tipo_campania, 
# MAGIC     (SELECT IFNULL(MAX(utm_channel), 'n/a') FROM gold_lakehouse.mapeo_origen_campania WHERE utm_source = origen_campania) AS canal_campania, 
# MAGIC     (SELECT IFNULL(MAX(utm_medium), 'n/a') FROM gold_lakehouse.mapeo_origen_campania WHERE utm_source = origen_campania) AS medio_campania  
# MAGIC FROM 
# MAGIC     silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_origen_campania
# MAGIC USING origen_campania_sales_view 
# MAGIC ON gold_lakehouse.dim_origen_campania.nombre_origen_campania = origen_campania_sales_view.nombre_origen_campania
# MAGIC WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_origen_campania.tipo_campania = origen_campania_sales_view.tipo_campania,  
# MAGIC gold_lakehouse.dim_origen_campania.canal_campania = origen_campania_sales_view.canal_campania, 
# MAGIC gold_lakehouse.dim_origen_campania.medio_campania = origen_campania_sales_view.medio_campania
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_origen_campania.nombre_origen_campania, gold_lakehouse.dim_origen_campania.tipo_campania, gold_lakehouse.dim_origen_campania.canal_campania, gold_lakehouse.dim_origen_campania.medio_campania)
# MAGIC VALUES (origen_campania_sales_view.nombre_origen_campania, origen_campania_sales_view.tipo_campania, origen_campania_sales_view.canal_campania, origen_campania_sales_view.medio_campania)
