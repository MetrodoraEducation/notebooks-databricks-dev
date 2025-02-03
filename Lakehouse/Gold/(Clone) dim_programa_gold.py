# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_programa_view AS
# MAGIC SELECT DISTINCT
# MAGIC     codigo_programa AS codPrograma,
# MAGIC     area_title AS nombrePrograma,
# MAGIC     degree_title AS tipoPrograma,
# MAGIC     entidad_legal AS entidadLegal,
# MAGIC     especialidad AS especialidad,
# MAGIC     vertical AS vertical,
# MAGIC     nombre_del_programa_oficial_completo AS nombreProgramaCompleto,
# MAGIC     MAX(TRY_CAST(fecha_creacion AS TIMESTAMP)) AS ETLcreatedDate,
# MAGIC     MAX(TRY_CAST(ultima_actualizacion AS TIMESTAMP)) AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.classlifetitulaciones
# MAGIC WHERE codigo_programa IS NOT NULL
# MAGIC GROUP BY codPrograma, nombrePrograma, tipoPrograma, entidadLegal, especialidad, vertical, nombreProgramaCompleto;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_programa_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_programa AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     codPrograma,
# MAGIC     nombrePrograma,
# MAGIC     tipoPrograma,
# MAGIC     entidadLegal,
# MAGIC     especialidad,
# MAGIC     vertical,
# MAGIC     nombreProgramaCompleto,
# MAGIC     ETLcreatedDate,
# MAGIC     ETLupdatedDate
# MAGIC   FROM dim_programa_view
# MAGIC ) AS source
# MAGIC ON target.codPrograma = source.codPrograma
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.nombrePrograma <> source.nombrePrograma OR
# MAGIC     target.tipoPrograma <> source.tipoPrograma OR
# MAGIC     target.entidadLegal <> source.entidadLegal OR
# MAGIC     target.especialidad <> source.especialidad OR
# MAGIC     target.vertical <> source.vertical OR
# MAGIC     target.nombreProgramaCompleto <> source.nombreProgramaCompleto
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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_lakehouse.dim_programa LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Validate duplicate >1
# MAGIC %sql
# MAGIC SELECT codPrograma, COUNT(*) AS total_duplicados
# MAGIC FROM gold_lakehouse.dim_programa
# MAGIC GROUP BY codPrograma
# MAGIC HAVING COUNT(*) > 1;
