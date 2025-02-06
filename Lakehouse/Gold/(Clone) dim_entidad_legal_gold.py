# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_ENTIDAD_LEGAL**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_entidad_legal_view AS
# MAGIC SELECT DISTINCT
# MAGIC     TRIM(institucion) AS nombreInstitucion
# MAGIC FROM silver_lakehouse.sales
# MAGIC WHERE institucion IS NOT NULL AND institucion <> '';
# MAGIC
# MAGIC select * from dim_entidad_legal_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_entidad_legal AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT 
# MAGIC         TRIM(institucion) AS nombreInstitucion,
# MAGIC         CASE 
# MAGIC             WHEN TRIM(institucion) = 'CESIF' THEN 'CF'
# MAGIC             WHEN TRIM(institucion) = 'ISEP' THEN 'IP'
# MAGIC             -- Agregar más reglas aquí si se identifican más códigos
# MAGIC             ELSE NULL 
# MAGIC         END AS codigoEntidadLegal,
# MAGIC         current_timestamp() AS ETLcreatedDate,
# MAGIC         current_timestamp() AS ETLupdatedDate
# MAGIC     FROM silver_lakehouse.sales
# MAGIC     WHERE institucion IS NOT NULL AND institucion <> ''
# MAGIC ) AS source
# MAGIC ON target.nombreInstitucion = source.nombreInstitucion
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.codigoEntidadLegal = source.codigoEntidadLegal,
# MAGIC         target.ETLupdatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombreInstitucion, codigoEntidadLegal, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombreInstitucion, source.codigoEntidadLegal, source.ETLcreatedDate, source.ETLupdatedDate);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold_lakehouse.dim_entidad_legal (nombreInstitucion, codigoEntidadLegal, ETLcreatedDate, ETLupdatedDate)
# MAGIC SELECT 
# MAGIC     t.nombreInstitucion, 
# MAGIC     t.codigoEntidadLegal, 
# MAGIC     current_timestamp(), 
# MAGIC     current_timestamp()
# MAGIC FROM (
# MAGIC     VALUES 
# MAGIC         ('CESIF', 'CF'),
# MAGIC         ('UNIVERSANIDAD', 'US'),
# MAGIC         ('OCEANO', 'OC'),
# MAGIC         ('ISEP', 'IP'),
# MAGIC         ('METRODORA LEARNING', 'ML'),
# MAGIC         ('TROPOS', 'TP'),
# MAGIC         ('PLAN EIR', 'PE'),
# MAGIC         ('IEPP', 'IE'),
# MAGIC         ('CIEP', 'CI'),
# MAGIC         ('METRODORA FP', 'MF'),
# MAGIC         ('SAIUS', 'SA'),
# MAGIC         ('ENTI', 'EN'),
# MAGIC         ('METRODORAFP ALBACETE', 'AB'),
# MAGIC         ('METRODORAFP AYALA', 'AY'),
# MAGIC         ('METRODORAFP CÁMARA', 'CA'),
# MAGIC         ('METRODORAFP EUSES', 'EU'),
# MAGIC         ('OCEANO EXPO-ZARAGOZA', 'EX'),
# MAGIC         ('OCEANO PORCHES-ZARAGOZA', 'PO'),
# MAGIC         ('METRODORAFP GIJÓN', 'GI'),
# MAGIC         ('METRODORAFP LOGROÑO', 'LO'),
# MAGIC         ('METRODORAFP SANTANDER', 'ST'),
# MAGIC         ('METRODORAFP VALLADOLID', 'VL'),
# MAGIC         ('METRODORAFP MADRID-RIO', 'RI')
# MAGIC ) AS t(nombreInstitucion, codigoEntidadLegal)
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM gold_lakehouse.dim_entidad_legal target 
# MAGIC     WHERE target.codigoEntidadLegal = t.codigoEntidadLegal
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW entidad_legal_view AS
# MAGIC SELECT DISTINCT 
# MAGIC     TRIM(entidadLegal) AS nombreInstitucion
# MAGIC FROM gold_lakehouse.dim_producto
# MAGIC WHERE entidadLegal IS NOT NULL AND entidadLegal <> ''
# MAGIC AND entidadLegal NOT IN (SELECT nombreInstitucion FROM gold_lakehouse.dim_entidad_legal);
# MAGIC
# MAGIC select * from entidad_legal_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_entidad_legal AS target
# MAGIC USING (
# MAGIC     SELECT nombreInstitucion, UPPER(SUBSTRING(nombreInstitucion, 1, 2)) AS codigoEntidadLegal
# MAGIC     FROM entidad_legal_view
# MAGIC ) AS source
# MAGIC ON target.nombreInstitucion = source.nombreInstitucion
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombreInstitucion, codigoEntidadLegal, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombreInstitucion, source.codigoEntidadLegal, current_timestamp(), current_timestamp());
