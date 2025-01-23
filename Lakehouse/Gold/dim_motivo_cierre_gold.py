# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW motivo_cierre_sales_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT motivo_cierre AS motivo_cierre
# MAGIC
# MAGIC FROM 
# MAGIC     silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_cierre
# MAGIC USING motivo_cierre_sales_view 
# MAGIC ON gold_lakehouse.dim_motivo_cierre.motivo_cierre = motivo_cierre_sales_view.motivo_cierre
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_motivo_cierre.motivo_cierre)
# MAGIC VALUES (motivo_cierre_sales_view.motivo_cierre)
