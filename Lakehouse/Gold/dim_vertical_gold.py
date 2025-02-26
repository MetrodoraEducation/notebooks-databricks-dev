# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_VERTICAL**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_vertical_view AS
# MAGIC     SELECT DISTINCT
# MAGIC         vertical AS nombreVertical
# MAGIC     FROM gold_lakehouse.dim_producto
# MAGIC     WHERE vertical IS NOT NULL 
# MAGIC     AND vertical != '';
# MAGIC
# MAGIC select * from dim_vertical_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `idDimVertical = -1` existe solo una vez con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_vertical AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombreVertical, 'n/a' AS nombreVerticalCorto
# MAGIC ) AS source
# MAGIC ON UPPER(target.nombreVertical) = 'n/a'
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombreVertical, nombreVerticalCorto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'n/a', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 2️⃣ 🔹 Insertar los valores predeterminados (`FP`, `HE`, `FC`) solo si no existen
# MAGIC MERGE INTO gold_lakehouse.dim_vertical AS target
# MAGIC USING (
# MAGIC     SELECT 'Formación Profesional' AS nombreVertical, 'FP' AS nombreVerticalCorto UNION ALL
# MAGIC     SELECT 'High Education', 'HE' UNION ALL
# MAGIC     SELECT 'Formación Continua', 'FC'
# MAGIC ) AS source
# MAGIC ON target.nombreVerticalCorto = source.nombreVerticalCorto
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombreVertical, nombreVerticalCorto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombreVertical, source.nombreVerticalCorto, current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 3️⃣ 🔹 Insertar nuevos valores desde `dim_vertical_view` sin alterar el registro `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_vertical AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT nombreVertical FROM dim_vertical_view WHERE nombreVertical <> 'n/a'
# MAGIC ) AS source
# MAGIC ON UPPER(target.nombreVertical) = UPPER(source.nombreVertical)
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombreVertical, nombreVerticalCorto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (
# MAGIC         source.nombreVertical, 
# MAGIC         TRIM(
# MAGIC             CONCAT(
# MAGIC                 UPPER(LEFT(element_at(SPLIT(source.nombreVertical, ' '), 1), 1)),  -- Primera letra de la primera palabra
# MAGIC                 UPPER(LEFT(element_at(SPLIT(source.nombreVertical, ' '), 2), 1))   -- Primera letra de la segunda palabra
# MAGIC             )
# MAGIC         ), 
# MAGIC         current_timestamp(), 
# MAGIC         current_timestamp()
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3️⃣ 🔹 Asegurar que solo hay un `n/a`
# MAGIC DELETE FROM gold_lakehouse.dim_vertical
# MAGIC WHERE nombreVertical = 'n/a' AND idDimVertical <> -1;
