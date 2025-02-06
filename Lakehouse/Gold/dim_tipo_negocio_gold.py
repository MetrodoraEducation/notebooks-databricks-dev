# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_TIPO_NEGOCIO**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tipo_negocio_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT tipo_negocio_desc AS tipo_negocio_desc,
# MAGIC     cod_tipo_negocio	AS cod_tipo_negocio
# MAGIC
# MAGIC FROM 
# MAGIC     gold_lakehouse.dim_estudio

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_negocio
# MAGIC USING tipo_negocio_view 
# MAGIC ON gold_lakehouse.dim_tipo_negocio.tipo_negocio_desc = tipo_negocio_view.tipo_negocio_desc
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_tipo_negocio.cod_tipo_negocio = tipo_negocio_view.cod_tipo_negocio
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_tipo_negocio.tipo_negocio_desc,gold_lakehouse.dim_tipo_negocio.cod_tipo_negocio)
# MAGIC VALUES (tipo_negocio_view.tipo_negocio_desc,tipo_negocio_view.cod_tipo_negocio)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_tipo_negocio_view AS
# MAGIC SELECT DISTINCT
# MAGIC     dp.tipoNegocio AS nombreTipoNegocio,
# MAGIC     CASE 
# MAGIC         WHEN UPPER(dp.tipoNegocio) = 'B2C' THEN 'C'
# MAGIC         WHEN UPPER(dp.tipoNegocio) = 'B2B' THEN 'B'
# MAGIC         ELSE 'N/A' -- Si hay valores inesperados
# MAGIC     END AS codigo,
# MAGIC     CURRENT_TIMESTAMP AS ETLcreatedDate,
# MAGIC     CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM gold_lakehouse.dim_producto dp
# MAGIC WHERE dp.tipoNegocio IS NOT NULL;
# MAGIC
# MAGIC select * from dim_tipo_negocio_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_tipo_negocio AS target
# MAGIC USING dim_tipo_negocio_view AS source
# MAGIC ON UPPER(target.tipo_negocio_desc) = UPPER(source.nombreTipoNegocio)
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP,
# MAGIC         target.cod_tipo_negocio = source.codigo,
# MAGIC         target.ETLcreatedDate = COALESCE(target.ETLcreatedDate, source.ETLcreatedDate)  -- Mantiene ETLcreatedDate
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (tipo_negocio_desc, cod_tipo_negocio, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombreTipoNegocio, source.codigo, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_tipo_negocio
