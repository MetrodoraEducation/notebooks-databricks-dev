# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW estado_venta_sales_view AS 
# MAGIC SELECT 
# MAGIC         DISTINCT 
# MAGIC                 CASE 
# MAGIC                     WHEN estado_venta = 'Abierta' THEN 'ABIERTA' 
# MAGIC                     WHEN estado_venta = 'Anulada' THEN 'ANULADA'
# MAGIC                     WHEN estado_venta = 'Cerrada' THEN 'CERRADA'
# MAGIC                 ELSE estado_venta
# MAGIC                 END nombre_estado_venta,
# MAGIC                 current_timestamp() AS ETLcreatedDate,
# MAGIC                 current_timestamp() AS ETLupdatedDate
# MAGIC     FROM silver_lakehouse.sales;
# MAGIC
# MAGIC select * from estado_venta_sales_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_estado_venta AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_estado_venta
# MAGIC ) AS source
# MAGIC ON target.id_dim_estado_venta = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_estado_venta, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_estado_venta, NULL, NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_estado_venta AS target
# MAGIC USING estado_venta_sales_view AS source
# MAGIC ON UPPER(target.nombre_estado_venta) = UPPER(source.nombre_estado_venta)
# MAGIC AND target.id_dim_estado_venta != -1  -- 🔹 Evita afectar el registro `-1`
# MAGIC
# MAGIC -- 🔹 **Si ya existe, lo actualiza (si aplica)**
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.nombre_estado_venta = source.nombre_estado_venta
# MAGIC
# MAGIC -- 🔹 **Si no existe, lo inserta**
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_estado_venta, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_estado_venta, source.ETLcreatedDate, source.ETLupdatedDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_estado_venta_view AS
# MAGIC SELECT DISTINCT
# MAGIC     CASE 
# MAGIC         WHEN fecha_hora_anulacion IS NOT NULL THEN 'ANULADA'
# MAGIC         WHEN fecha_hora_anulacion IS NULL AND etapa like '%Matriculado' THEN 'CERRADA'
# MAGIC         ELSE 'ABIERTA'
# MAGIC     END AS nombre_estado_venta
# MAGIC FROM silver_lakehouse.zohodeals;
# MAGIC
# MAGIC select * from dim_estado_venta_view;

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
