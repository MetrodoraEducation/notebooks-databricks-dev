# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_PROGRAMA**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_programa_view AS
# MAGIC SELECT DISTINCT
# MAGIC     codigo_programa AS codPrograma,
# MAGIC     TRIM(UPPER(area_title)) AS nombrePrograma,
# MAGIC     TRIM(UPPER(degree_title)) AS tipoPrograma,
# MAGIC     TRIM(UPPER(entidad_legal)) AS entidadLegal,
# MAGIC     TRIM(UPPER(especialidad)) AS especialidad,
# MAGIC     TRIM(UPPER(vertical)) AS vertical,
# MAGIC     TRIM(UPPER(nombre_del_programa_oficial_completo)) AS nombreProgramaCompleto,
# MAGIC     MAX(TRY_CAST(fecha_creacion AS TIMESTAMP)) AS ETLcreatedDate,
# MAGIC     MAX(TRY_CAST(ultima_actualizacion AS TIMESTAMP)) AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.classlifetitulaciones
# MAGIC WHERE codigo_programa IS NOT NULL
# MAGIC GROUP BY 
# MAGIC     codPrograma, nombrePrograma, tipoPrograma, entidadLegal, especialidad, vertical, nombreProgramaCompleto;
# MAGIC
# MAGIC select * from dim_programa_view limit 25

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_programa AS target
# MAGIC USING dim_programa_view AS source
# MAGIC ON UPPER(TRIM(target.codPrograma)) = UPPER(TRIM(source.codPrograma)) 
# MAGIC    AND UPPER(TRIM(target.nombreProgramaCompleto)) = UPPER(TRIM(source.nombreProgramaCompleto))
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     COALESCE(TRIM(UPPER(target.nombrePrograma)), '') <> COALESCE(TRIM(UPPER(source.nombrePrograma)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.tipoPrograma)), '') <> COALESCE(TRIM(UPPER(source.tipoPrograma)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.entidadLegal)), '') <> COALESCE(TRIM(UPPER(source.entidadLegal)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.especialidad)), '') <> COALESCE(TRIM(UPPER(source.especialidad)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.vertical)), '') <> COALESCE(TRIM(UPPER(source.vertical)), '') OR
# MAGIC     COALESCE(TRIM(UPPER(target.nombreProgramaCompleto)), '') <> COALESCE(TRIM(UPPER(source.nombreProgramaCompleto)), '')
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.nombrePrograma = source.nombrePrograma,
# MAGIC     target.tipoPrograma = source.tipoPrograma,
# MAGIC     target.entidadLegal = source.entidadLegal,
# MAGIC     target.especialidad = source.especialidad,
# MAGIC     target.vertical = source.vertical,
# MAGIC     target.nombreProgramaCompleto = source.nombreProgramaCompleto,
# MAGIC     target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     codPrograma, nombrePrograma, tipoPrograma, entidadLegal, especialidad, vertical, nombreProgramaCompleto, ETLcreatedDate, ETLupdatedDate
# MAGIC   ) 
# MAGIC   VALUES (
# MAGIC     source.codPrograma, source.nombrePrograma, source.tipoPrograma, source.entidadLegal, source.especialidad, source.vertical, 
# MAGIC     source.nombreProgramaCompleto, source.ETLcreatedDate, source.ETLupdatedDate
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,Validate duplicate >1
# MAGIC %sql
# MAGIC SELECT codPrograma, COUNT(*) AS total_duplicados
# MAGIC FROM gold_lakehouse.dim_programa
# MAGIC GROUP BY codPrograma
# MAGIC HAVING COUNT(*) > 1;
