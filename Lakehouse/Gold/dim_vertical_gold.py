# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_VERTICAL**

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold_lakehouse.dim_vertical (nombreVertical, nombreVerticalCorto)
# MAGIC SELECT 'Formación Profesional', 'FP' 
# MAGIC WHERE NOT EXISTS (SELECT 1 FROM gold_lakehouse.dim_vertical WHERE nombreVerticalCorto = 'FP');
# MAGIC
# MAGIC INSERT INTO gold_lakehouse.dim_vertical (nombreVertical, nombreVerticalCorto)
# MAGIC SELECT 'High Education', 'HE' 
# MAGIC WHERE NOT EXISTS (SELECT 1 FROM gold_lakehouse.dim_vertical WHERE nombreVerticalCorto = 'HE');
# MAGIC
# MAGIC INSERT INTO gold_lakehouse.dim_vertical (nombreVertical, nombreVerticalCorto)
# MAGIC SELECT 'Formación Continua', 'FC' 
# MAGIC WHERE NOT EXISTS (SELECT 1 FROM gold_lakehouse.dim_vertical WHERE nombreVerticalCorto = 'FC');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_vertical_view AS
# MAGIC SELECT DISTINCT
# MAGIC     vertical AS nombreVertical
# MAGIC FROM gold_lakehouse.dim_producto
# MAGIC WHERE vertical IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_vertical_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_vertical AS target
# MAGIC USING (
# MAGIC   SELECT DISTINCT nombreVertical FROM dim_vertical_view
# MAGIC ) AS source
# MAGIC ON target.nombreVertical = source.nombreVertical
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (nombreVertical, nombreVerticalCorto, ETLcreatedDate, ETLupdatedDate)
# MAGIC   VALUES (
# MAGIC     source.nombreVertical, 
# MAGIC     TRIM(
# MAGIC       CONCAT(
# MAGIC         UPPER(LEFT(element_at(SPLIT(source.nombreVertical, ' '), 1), 1)),  -- Primera letra de la primera palabra
# MAGIC         UPPER(LEFT(element_at(SPLIT(source.nombreVertical, ' '), 2), 1))   -- Primera letra de la segunda palabra
# MAGIC       )
# MAGIC     ), 
# MAGIC     current_timestamp(), 
# MAGIC     current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT p.vertical 
# MAGIC FROM gold_lakehouse.dim_producto p
# MAGIC LEFT JOIN gold_lakehouse.dim_vertical v ON p.vertical = v.nombreVertical
# MAGIC WHERE v.nombreVertical IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_lakehouse.dim_vertical;
