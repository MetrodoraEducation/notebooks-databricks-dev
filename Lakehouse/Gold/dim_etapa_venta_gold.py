# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW etapa_venta_sales_view
# MAGIC     AS SELECT distinct etapa_venta as nombre_etapa_venta    
# MAGIC     FROM silver_lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_etapa_venta
# MAGIC USING etapa_venta_sales_view 
# MAGIC ON gold_lakehouse.dim_etapa_venta.nombre_etapa_venta = etapa_venta_sales_view.nombre_etapa_venta
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_etapa_venta.nombre_etapa_venta)
# MAGIC VALUES (etapa_venta_sales_view.nombre_etapa_venta)
