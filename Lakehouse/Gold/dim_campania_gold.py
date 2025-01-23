# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW campania_sales_view
# MAGIC     AS SELECT distinct campania as nombre_campania     
# MAGIC     FROM silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_campania
# MAGIC USING campania_sales_view 
# MAGIC ON gold_lakehouse.dim_campania.nombre_campania = campania_sales_view.nombre_campania
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_campania.nombre_campania)
# MAGIC VALUES (campania_sales_view.nombre_campania)
