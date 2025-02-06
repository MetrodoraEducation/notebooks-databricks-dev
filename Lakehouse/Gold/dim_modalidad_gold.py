# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_MODALIDAD**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW modalidad_sales_view AS 
# MAGIC SELECT DISTINCT 
# MAGIC     COALESCE(
# MAGIC         (SELECT IFNULL(MAX(modalidad_norm), 'n/a') 
# MAGIC          FROM gold_lakehouse.mapeo_modalidad 
# MAGIC          WHERE UPPER(gold_lakehouse.mapeo_modalidad.modalidad) = UPPER(TRIM(sales.modalidad))),
# MAGIC         UPPER(TRIM(dp.modalidad))
# MAGIC     ) AS nombre_modalidad,
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT IFNULL(MAX(modalidad_norm), 'n/a') 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(gold_lakehouse.mapeo_modalidad.modalidad) = UPPER(TRIM(sales.modalidad))),
# MAGIC             UPPER(TRIM(dp.modalidad))
# MAGIC         ) = 'ON PREMISE' THEN 'P' 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT IFNULL(MAX(modalidad_norm), 'n/a') 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(gold_lakehouse.mapeo_modalidad.modalidad) = UPPER(TRIM(sales.modalidad))),
# MAGIC             UPPER(TRIM(dp.modalidad))
# MAGIC         ) = 'ONLINE' THEN 'O' 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT IFNULL(MAX(modalidad_norm), 'n/a') 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(gold_lakehouse.mapeo_modalidad.modalidad) = UPPER(TRIM(sales.modalidad))),
# MAGIC             UPPER(TRIM(dp.modalidad))
# MAGIC         ) = 'BLENDED' THEN 'B'
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT IFNULL(MAX(modalidad_norm), 'n/a') 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(gold_lakehouse.mapeo_modalidad.modalidad) = UPPER(TRIM(sales.modalidad))),
# MAGIC             UPPER(TRIM(dp.modalidad))
# MAGIC         ) = 'STREAMING' THEN 'S' 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT IFNULL(MAX(modalidad_norm), 'n/a') 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(gold_lakehouse.mapeo_modalidad.modalidad) = UPPER(TRIM(sales.modalidad))),
# MAGIC             UPPER(TRIM(dp.modalidad))
# MAGIC         ) = 'ONLINE+PRACTICAS' THEN 'J'
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT IFNULL(MAX(modalidad_norm), 'n/a') 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(gold_lakehouse.mapeo_modalidad.modalidad) = UPPER(TRIM(sales.modalidad))),
# MAGIC             UPPER(TRIM(dp.modalidad))
# MAGIC         ) = 'ONLINE+CLASES' THEN 'C' 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT IFNULL(MAX(modalidad_norm), 'n/a') 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(gold_lakehouse.mapeo_modalidad.modalidad) = UPPER(TRIM(sales.modalidad))),
# MAGIC             UPPER(TRIM(dp.modalidad))
# MAGIC         ) = 'SEMIPRESENCIAL' THEN 'M' 
# MAGIC         ELSE 'n/a' 
# MAGIC     END AS codigo
# MAGIC FROM 
# MAGIC     silver_lakehouse.sales sales
# MAGIC LEFT JOIN 
# MAGIC     gold_lakehouse.dim_producto dp
# MAGIC ON 
# MAGIC     UPPER(TRIM(sales.modalidad)) = UPPER(TRIM(dp.modalidad))
# MAGIC WHERE 
# MAGIC     COALESCE(UPPER(TRIM(sales.modalidad)), UPPER(TRIM(dp.modalidad))) IS NOT NULL;
# MAGIC
# MAGIC select * from modalidad_sales_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_modalidad AS target
# MAGIC USING modalidad_sales_view AS source
# MAGIC ON UPPER(target.nombre_modalidad) = UPPER(source.nombre_modalidad)
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.codigo = source.codigo,
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP,
# MAGIC         target.ETLcreatedDate = COALESCE(target.ETLcreatedDate, CURRENT_TIMESTAMP)  -- Evita sobrescribir ETLcreatedDate
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_modalidad, codigo, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_modalidad, source.codigo, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_modalidad
