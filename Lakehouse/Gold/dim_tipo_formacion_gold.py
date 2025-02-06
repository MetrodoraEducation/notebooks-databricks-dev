# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_TIPO_FORMACION**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tipo_formacion_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT tipo_formacion_desc AS tipo_formacion_desc,
# MAGIC     cod_tipo_formacion	AS cod_tipo_formacion
# MAGIC
# MAGIC FROM 
# MAGIC     gold_lakehouse.dim_estudio

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_formacion
# MAGIC USING tipo_formacion_view 
# MAGIC ON gold_lakehouse.dim_tipo_formacion.tipo_formacion_desc = tipo_formacion_view.tipo_formacion_desc
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_tipo_formacion.cod_tipo_formacion = tipo_formacion_view.cod_tipo_formacion
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_tipo_formacion.tipo_formacion_desc,gold_lakehouse.dim_tipo_formacion.cod_tipo_formacion)
# MAGIC VALUES (tipo_formacion_view.tipo_formacion_desc,tipo_formacion_view.cod_tipo_formacion)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_tipo_formacion_view AS
# MAGIC SELECT DISTINCT
# MAGIC     UPPER(TRIM(dp.tipoProducto)) AS nombreTipoFormacion,
# MAGIC     CASE 
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'ACCESO A ESPECIALIDAD' THEN '01'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'MÁSTER' THEN '02'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'CURSO POSTGRADO' THEN '03'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'CURSO EXPERTO' THEN '04'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'TALLER/SEMINARIO' THEN '05'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'PREGRADO' THEN '06'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'OPOSICIÓN' THEN '07'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'OTROS CURSOS' THEN '08'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'FORMACIÓN CONTINUA' THEN '09'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'EXECUTIVE EDUCATION' THEN '10'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'PROGRAMA DESARROLLO DIRECTIVO (PDD)' THEN '11'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'CFGS' THEN '12'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'CFGM' THEN '13'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'GRADO' THEN '14'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'CERTIFICADO PROFESIONALIDAD' THEN '15'
# MAGIC         WHEN UPPER(TRIM(dp.tipoProducto)) = 'CURSO ESPECIALIZACIÓN' THEN '16'
# MAGIC         ELSE '00' -- GENERAL o valores no contemplados
# MAGIC     END AS codTipoFormacion,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM gold_lakehouse.dim_producto dp
# MAGIC WHERE dp.tipoProducto IS NOT NULL AND dp.tipoProducto <> '';
# MAGIC
# MAGIC select * from dim_tipo_formacion_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_formacion AS target
# MAGIC USING dim_tipo_formacion_view AS source
# MAGIC ON UPPER(target.tipo_formacion_desc) = UPPER(source.nombreTipoFormacion) -- Cambio aquí
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.cod_tipo_formacion = source.codTipoFormacion,  -- Cambio aquí
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (tipo_formacion_desc, cod_tipo_formacion, ETLcreatedDate, ETLupdatedDate) -- Cambio aquí
# MAGIC     VALUES (source.nombreTipoFormacion, source.codTipoFormacion, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  gold_lakehouse.dim_tipo_formacion
