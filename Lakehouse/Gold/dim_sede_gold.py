# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sede_sales_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT 
# MAGIC     (SELECT IFNULL(MAX(sede_norm), 'n/a') FROM gold_lakehouse.mapeo_sede WHERE gold_lakehouse.mapeo_sede.sede = silver_lakehouse.sales.sede) AS nombre_sede,
# MAGIC     case when nombre_sede='ALICANTE' then 'ALI'
# MAGIC     when nombre_sede='BARCELONA' then 'BCN'
# MAGIC     when nombre_sede='BILBAO' then 'BIL'
# MAGIC     when nombre_sede='DISTANCIA' then 'DIS'
# MAGIC     when nombre_sede='GIJÓN INMACULADA' then 'GIM'
# MAGIC     when nombre_sede='GIJÓN SANTO ÁNGEL' then 'GSA'
# MAGIC     when nombre_sede='HUESCA' then 'HUE'
# MAGIC     when nombre_sede='IRUN' then 'IRU'
# MAGIC     when nombre_sede='JACA' then 'JAC'
# MAGIC     when nombre_sede='LA CORUÑA' then 'LCR'
# MAGIC     when nombre_sede='LAS PALMAS DE GRAN CANARIA' then 'LPM'
# MAGIC     when nombre_sede='LOGROÑO' then 'LOG'
# MAGIC     when nombre_sede='MADRID' then 'MAD'
# MAGIC     when nombre_sede='MADRID AYALA' then 'MAY'
# MAGIC     when nombre_sede='MADRID CAMARA' then 'MCA'
# MAGIC     when nombre_sede='MADRID RIO' then 'MRI'
# MAGIC     when nombre_sede='MALLORCA' then 'MLL'
# MAGIC     when nombre_sede='MURCIA' then 'MUR'
# MAGIC     when nombre_sede='PAMPLONA' then 'PAM'
# MAGIC     when nombre_sede='PARIS' then 'PAR'
# MAGIC     when nombre_sede='SANTA CRUZ DE TENERIFE' then 'SCT'
# MAGIC     when nombre_sede='SANTANDER' then 'SAN'
# MAGIC     when nombre_sede='SANTANDER MONTES CARPATOS' then 'SMC'
# MAGIC     when nombre_sede='SANTANDER VIA CARPETANA' then 'SVC'
# MAGIC     when nombre_sede='SEVILLA' then 'SEV'
# MAGIC     when nombre_sede='VALENCIA' then 'VAL'
# MAGIC     when nombre_sede='VALLADOLID' then 'VLL'
# MAGIC     when nombre_sede='VITORIA' then 'VIT'
# MAGIC     when nombre_sede='ZARAGOZA' then 'ZGZ'
# MAGIC     when nombre_sede='ZARAGOZA EXPO 3D' then 'Z3D'
# MAGIC     when nombre_sede='ZARAGOZA EXPO 5D' then 'Z5D'
# MAGIC     when nombre_sede='ZARAGOZA PORCHES' then 'ZPC'
# MAGIC     when nombre_sede='MEXICO' then 'MEX' 
# MAGIC     when nombre_sede='CÓRDOBA' then 'COR'
# MAGIC     when nombre_sede='ALBACETE' then 'ALB' 
# MAGIC     else 'n/a' end AS codigo_sede
# MAGIC
# MAGIC
# MAGIC FROM 
# MAGIC     silver_lakehouse.sales

# COMMAND ----------

# DBTITLE 1,Merge Into from sales
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_sede
# MAGIC USING sede_sales_view 
# MAGIC ON gold_lakehouse.dim_sede.nombre_sede = sede_sales_view.nombre_sede
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_sede.codigo_sede = sede_sales_view.codigo_sede
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_sede.nombre_sede,gold_lakehouse.dim_sede.codigo_sede)
# MAGIC VALUES (sede_sales_view.nombre_sede,sede_sales_view.codigo_sede)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_lakehouse.dim_sede;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_sede_view AS
# MAGIC SELECT DISTINCT 
# MAGIC     UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) AS codigo_sede,
# MAGIC     CASE 
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'ALI' THEN 'ALICANTE'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'BCN' THEN 'BARCELONA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'BIL' THEN 'BILBAO'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'DIS' THEN 'DISTANCIA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'GIM' THEN 'GIJÓN INMACULADA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'GSA' THEN 'GIJÓN SANTO ÁNGEL'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'HUE' THEN 'HUESCA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'IRU' THEN 'IRUN'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'JAC' THEN 'JACA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'LCR' THEN 'LA CORUÑA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'LPM' THEN 'LAS PALMAS DE GRAN CANARIA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'LOG' THEN 'LOGROÑO'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'MAD' THEN 'MADRID'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'MAY' THEN 'MADRID AYALA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'MCA' THEN 'MADRID CAMARA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'MRI' THEN 'MADRID RIO'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'MLL' THEN 'MALLORCA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'MUR' THEN 'MURCIA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'PAM' THEN 'PAMPLONA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'PAR' THEN 'PARIS'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'SCT' THEN 'SANTA CRUZ DE TENERIFE'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'SAN' THEN 'SANTANDER'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'SMC' THEN 'SANTANDER MONTES CARPATOS'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'SVC' THEN 'SANTANDER VIA CARPETANA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'SEV' THEN 'SEVILLA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'VAL' THEN 'VALENCIA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'VLL' THEN 'VALLADOLID'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'VIT' THEN 'VITORIA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'ZGZ' THEN 'ZARAGOZA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'Z3D' THEN 'ZARAGOZA EXPO 3D'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'Z5D' THEN 'ZARAGOZA EXPO 5D'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'ZPC' THEN 'ZARAGOZA PORCHES'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'MEX' THEN 'MEXICO'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'COR' THEN 'CÓRDOBA'
# MAGIC         WHEN UPPER(REGEXP_REPLACE(try_element_at(SPLIT(codProducto, '-'), 3), '[0-9]', '')) = 'ALB' THEN 'ALBACETE'
# MAGIC         ELSE 'n/a' 
# MAGIC     END AS nombre_sede
# MAGIC FROM gold_lakehouse.dim_producto
# MAGIC WHERE codProducto IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_sede_view

# COMMAND ----------

# DBTITLE 1,Merge into from dim_producto
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_sede AS target
# MAGIC USING (
# MAGIC   SELECT DISTINCT codigo_sede, nombre_sede
# MAGIC   FROM dim_sede_view
# MAGIC   WHERE codigo_sede IS NOT NULL AND nombre_sede IS NOT NULL
# MAGIC ) AS source
# MAGIC ON target.codigo_sede = source.codigo_sede
# MAGIC
# MAGIC WHEN MATCHED AND target.nombre_sede <> source.nombre_sede THEN
# MAGIC UPDATE SET 
# MAGIC   target.nombre_sede = source.nombre_sede
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (codigo_sede, nombre_sede)
# MAGIC VALUES (source.codigo_sede, source.nombre_sede);
