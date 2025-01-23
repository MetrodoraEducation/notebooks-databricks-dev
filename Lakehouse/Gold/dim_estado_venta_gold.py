# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW estado_venta_sales_view
# MAGIC     AS SELECT distinct estado_venta as nombre_estado_venta     
# MAGIC     FROM silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_estado_venta
# MAGIC USING estado_venta_sales_view 
# MAGIC ON gold_lakehouse.dim_estado_venta.nombre_estado_venta = estado_venta_sales_view.nombre_estado_venta
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_estado_venta.nombre_estado_venta)
# MAGIC VALUES (estado_venta_sales_view.nombre_estado_venta)
