# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW estado_venta_sales_view
# MAGIC     AS SELECT distinct estado_venta as nombre_estado_venta     
# MAGIC     FROM silver_lakehouse.sales;
# MAGIC
# MAGIC select * from estado_venta_sales_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_estado_venta
# MAGIC USING estado_venta_sales_view 
# MAGIC ON gold_lakehouse.dim_estado_venta.nombre_estado_venta = estado_venta_sales_view.nombre_estado_venta
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_estado_venta.nombre_estado_venta)
# MAGIC VALUES (estado_venta_sales_view.nombre_estado_venta)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_estado_venta_view AS
# MAGIC SELECT DISTINCT
# MAGIC     CASE 
# MAGIC         WHEN fecha_hora_anulacion IS NOT NULL THEN 'Anulada'
# MAGIC         WHEN fecha_hora_anulacion IS NULL AND etapa like '%Matriculado' THEN 'Cerrada'
# MAGIC         ELSE 'Abierta'
# MAGIC     END AS nombre_estado_venta
# MAGIC FROM silver_lakehouse.zohodeals;
# MAGIC
# MAGIC select * from dim_estado_venta_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_estado_venta AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         nombre_estado_venta, 
# MAGIC         current_timestamp() AS ETLcreatedDate,
# MAGIC         current_timestamp() AS ETLupdatedDate
# MAGIC     FROM dim_estado_venta_view
# MAGIC ) AS source
# MAGIC ON target.nombre_estado_venta = source.nombre_estado_venta
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET target.ETLupdatedDate = source.ETLupdatedDate
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_estado_venta, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_estado_venta, source.ETLcreatedDate, source.ETLupdatedDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_estado_venta
