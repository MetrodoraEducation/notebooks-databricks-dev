# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_ESPECIALIDAD**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_especialidad_view AS
# MAGIC SELECT DISTINCT
# MAGIC     UPPER(TRIM(dp.especialidad)) AS nombreEspecialidad,
# MAGIC     CASE 
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'MEDICINA' THEN 'MD'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'ENFERMERÍA' THEN 'NU'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'PSICOLOGÍA' THEN 'PS'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'FARMACIA' THEN 'PH'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'FISIOTERAPIA' THEN 'PT'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'OPTICA' THEN 'OP'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'BIOLOGÍA' THEN 'BI'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'MISCELANEA' THEN 'MX'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'ODONTOLOGIA' THEN 'OD'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'QUÍMICA' THEN 'CH'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'NUTRICIÓN' THEN 'NT'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'INFORMÁTICA' THEN 'IT'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'DEPORTE' THEN 'SP'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'ADMINISTRACIÓN' THEN 'AD'
# MAGIC         WHEN UPPER(TRIM(dp.especialidad)) = 'GENERAL' THEN '00'
# MAGIC         ELSE 'n/a'  -- General o sin coincidencia
# MAGIC     END AS codEspecialidad,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM gold_lakehouse.dim_producto dp
# MAGIC WHERE dp.especialidad IS NOT NULL AND dp.especialidad <> '';
# MAGIC
# MAGIC select * from dim_especialidad_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_especialidad AS target
# MAGIC USING dim_especialidad_view AS source
# MAGIC ON UPPER(target.nombreEspecialidad) = UPPER(source.nombreEspecialidad)
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.codEspecialidad = source.codEspecialidad,
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombreEspecialidad, codEspecialidad, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombreEspecialidad, source.codEspecialidad, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_especialidad
