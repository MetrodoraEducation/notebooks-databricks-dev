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

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_sede
# MAGIC USING sede_sales_view 
# MAGIC ON gold_lakehouse.dim_sede.nombre_sede = sede_sales_view.nombre_sede
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_sede.codigo_sede = sede_sales_view.codigo_sede
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_sede.nombre_sede,gold_lakehouse.dim_sede.codigo_sede)
# MAGIC VALUES (sede_sales_view.nombre_sede,sede_sales_view.codigo_sede)
