# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_estudio_view
# MAGIC     AS SELECT * FROM silver_lakehouse.dim_estudio

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_estudio_prueba AS target
# MAGIC USING dim_estudio_view AS source
# MAGIC ON target.cod_estudio = source.cod_estudio
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET 
# MAGIC     target.nombre_de_programa = source.nombre_de_programa,
# MAGIC     target.cod_vertical = source.cod_vertical,
# MAGIC     target.vertical_desc = source.vertical_desc,
# MAGIC     target.cod_entidad_legal = source.cod_entidad_legal,
# MAGIC     target.entidad_legal_desc = source.entidad_legal_desc,
# MAGIC     target.cod_especialidad = source.cod_especialidad,
# MAGIC     target.especialidad_desc = source.especialidad_desc,
# MAGIC     target.cod_tipo_formacion = source.cod_tipo_formacion,
# MAGIC     target.tipo_formacion_desc = source.tipo_formacion_desc,
# MAGIC     target.cod_tipo_negocio = source.cod_tipo_negocio,
# MAGIC     target.tipo_negocio_desc = source.tipo_negocio_desc
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (
# MAGIC     cod_estudio,
# MAGIC     nombre_de_programa,
# MAGIC     cod_vertical,
# MAGIC     vertical_desc,
# MAGIC     cod_entidad_legal,
# MAGIC     entidad_legal_desc,
# MAGIC     cod_especialidad,
# MAGIC     especialidad_desc,
# MAGIC     cod_tipo_formacion,
# MAGIC     tipo_formacion_desc,
# MAGIC     cod_tipo_negocio,
# MAGIC     tipo_negocio_desc
# MAGIC   ) VALUES (
# MAGIC     source.cod_estudio,
# MAGIC     source.nombre_de_programa,
# MAGIC     source.cod_vertical,
# MAGIC     source.vertical_desc,
# MAGIC     source.cod_entidad_legal,
# MAGIC     source.entidad_legal_desc,
# MAGIC     source.cod_especialidad,
# MAGIC     source.especialidad_desc,
# MAGIC     source.cod_tipo_formacion,
# MAGIC     source.tipo_formacion_desc,
# MAGIC     source.cod_tipo_negocio,
# MAGIC     source.tipo_negocio_desc)
