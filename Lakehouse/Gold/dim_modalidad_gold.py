# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_MODALIDAD**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW modalidad_sales_view AS 
# MAGIC WITH modalidad_union AS (
# MAGIC     -- 🔹 Extraemos todas las modalidades de ambas tablas
# MAGIC     SELECT DISTINCT 
# MAGIC         TRIM(UPPER(sales.modalidad)) AS modalidad
# MAGIC     FROM silver_lakehouse.sales sales
# MAGIC     FULL OUTER JOIN gold_lakehouse.dim_producto dp
# MAGIC     ON UPPER(TRIM(dp.modalidad)) = UPPER(TRIM(sales.modalidad))
# MAGIC     WHERE COALESCE(sales.modalidad, dp.modalidad) IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT 
# MAGIC     -- 🔹 Normalizamos la modalidad con `mapeo_modalidad`
# MAGIC     COALESCE(
# MAGIC         (SELECT MAX(modalidad_norm) 
# MAGIC          FROM gold_lakehouse.mapeo_modalidad 
# MAGIC          WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC         m.modalidad,  -- Si no hay mapeo, se mantiene el original
# MAGIC         'n/a'         -- Si todo es NULL, asigna 'n/a'
# MAGIC     ) AS nombre_modalidad,
# MAGIC
# MAGIC     -- 🔹 Mapeo de códigos basado en la modalidad normalizada
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%ON PREMISE%' THEN 'P' 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%ONLINE%' THEN 'O' 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%BLENDED%' THEN 'B'
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%STREAMING%' THEN 'S' 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%ONLINE+PRACTICAS%' THEN 'J'
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%ONLINE+CLASES%' THEN 'C' 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%SEMIPRESENCIAL%' THEN 'M' 
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%HÍBRIDA%' THEN 'H'
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%MADRID%' THEN 'MAD'
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%MODALIDAD: ENR%' THEN 'E'
# MAGIC         WHEN COALESCE(
# MAGIC             (SELECT MAX(modalidad_norm) 
# MAGIC              FROM gold_lakehouse.mapeo_modalidad 
# MAGIC              WHERE UPPER(TRIM(gold_lakehouse.mapeo_modalidad.modalidad)) = UPPER(TRIM(m.modalidad))),
# MAGIC             m.modalidad
# MAGIC         ) LIKE '%SANTANDER%' THEN 'STD' 
# MAGIC         ELSE 'n/a' 
# MAGIC     END AS codigo
# MAGIC
# MAGIC FROM modalidad_union m;
# MAGIC
# MAGIC -- 🔹 Validamos la vista
# MAGIC SELECT * FROM modalidad_sales_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ Asegurar que el ID -1 solo exista una vez con valores 'n/a'
# MAGIC MERGE INTO gold_lakehouse.dim_modalidad AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_modalidad, 'n/a' AS codigo, CURRENT_TIMESTAMP AS ETLcreatedDate, CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC ) AS source
# MAGIC ON target.id_dim_modalidad = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_modalidad, codigo, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'n/a', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC
# MAGIC -- 2️⃣ Actualizar registros existentes si cambia el código, sin afectar ETLcreatedDate
# MAGIC MERGE INTO gold_lakehouse.dim_modalidad AS target
# MAGIC USING modalidad_sales_view AS source
# MAGIC ON UPPER(target.nombre_modalidad) = UPPER(source.nombre_modalidad)
# MAGIC AND target.id_dim_modalidad != -1  -- 🔹 Evita actualizar el ID -1
# MAGIC
# MAGIC WHEN MATCHED AND target.codigo <> source.codigo THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.codigo = source.codigo,
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP;
# MAGIC
# MAGIC -- 3️⃣ Insertar nuevos registros sin tocar el id_dim_modalidad
# MAGIC MERGE INTO gold_lakehouse.dim_modalidad AS target
# MAGIC USING modalidad_sales_view AS source
# MAGIC ON UPPER(target.nombre_modalidad) = UPPER(source.nombre_modalidad)
# MAGIC AND target.id_dim_modalidad != -1  -- 🔹 Evita modificar el -1
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_modalidad, codigo, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_modalidad, source.codigo, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC
# MAGIC -- 4️⃣ Eliminar duplicados de 'n/a' si aparecen por error
# MAGIC DELETE FROM gold_lakehouse.dim_modalidad
# MAGIC WHERE nombre_modalidad = 'n/a' AND id_dim_modalidad <> -1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_modalidad
