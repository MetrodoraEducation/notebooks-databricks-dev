# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_ENTIDAD_LEGAL**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_entidad_legal_view AS
# MAGIC SELECT DISTINCT
# MAGIC     codProducto,
# MAGIC     CASE 
# MAGIC         WHEN codProducto LIKE '%CF%' THEN 'CESIF'
# MAGIC         WHEN codProducto LIKE '%US%' THEN 'UNIVERSANIDAD'
# MAGIC         WHEN codProducto LIKE '%OC%' THEN 'OCEANO'
# MAGIC         WHEN codProducto LIKE '%IP%' THEN 'ISEP'
# MAGIC         WHEN codProducto LIKE '%ML%' THEN 'METRODORA LEARNING'
# MAGIC         WHEN codProducto LIKE '%TP%' THEN 'TROPOS'
# MAGIC         WHEN codProducto LIKE '%PE%' THEN 'PLAN EIR'
# MAGIC         WHEN codProducto LIKE '%IE%' THEN 'IEPP'
# MAGIC         WHEN codProducto LIKE '%CI%' THEN 'CIEP'
# MAGIC         WHEN codProducto LIKE '%MF%' THEN 'METRODORA FP'
# MAGIC         WHEN codProducto LIKE '%SA%' THEN 'SAIUS'
# MAGIC         WHEN codProducto LIKE '%EN%' THEN 'ENTI'
# MAGIC         WHEN codProducto LIKE '%AB%' THEN 'METRODORAFP ALBACETE'
# MAGIC         WHEN codProducto LIKE '%AY%' THEN 'METRODORAFP AYALA'
# MAGIC         WHEN codProducto LIKE '%CA%' THEN 'METRODORAFP CÁMARA'
# MAGIC         WHEN codProducto LIKE '%EU%' THEN 'METRODORAFP EUSES'
# MAGIC         WHEN codProducto LIKE '%EX%' THEN 'OCEANO EXPO-ZARAGOZA'
# MAGIC         WHEN codProducto LIKE '%PO%' THEN 'OCEANO PORCHES-ZARAGOZA'
# MAGIC         WHEN codProducto LIKE '%GI%' THEN 'METRODORAFP GIJÓN'
# MAGIC         WHEN codProducto LIKE '%LO%' THEN 'METRODORAFP LOGROÑO'
# MAGIC         WHEN codProducto LIKE '%ST%' THEN 'METRODORAFP SANTANDER'
# MAGIC         WHEN codProducto LIKE '%VL%' THEN 'METRODORAFP VALLADOLID'
# MAGIC         WHEN codProducto LIKE '%RI%' THEN 'METRODORAFP MADRID-RIO'
# MAGIC         ELSE 'n/a' 
# MAGIC     END AS nombreInstitucion,
# MAGIC     CASE 
# MAGIC         WHEN codProducto LIKE '%CF%' THEN 'CF'
# MAGIC         WHEN codProducto LIKE '%US%' THEN 'US'
# MAGIC         WHEN codProducto LIKE '%OC%' THEN 'OC'
# MAGIC         WHEN codProducto LIKE '%IP%' THEN 'IP'
# MAGIC         WHEN codProducto LIKE '%ML%' THEN 'ML'
# MAGIC         WHEN codProducto LIKE '%TP%' THEN 'TP'
# MAGIC         WHEN codProducto LIKE '%PE%' THEN 'PE'
# MAGIC         WHEN codProducto LIKE '%IE%' THEN 'IE'
# MAGIC         WHEN codProducto LIKE '%CI%' THEN 'CI'
# MAGIC         WHEN codProducto LIKE '%MF%' THEN 'MF'
# MAGIC         WHEN codProducto LIKE '%SA%' THEN 'SA'
# MAGIC         WHEN codProducto LIKE '%EN%' THEN 'EN'
# MAGIC         WHEN codProducto LIKE '%AB%' THEN 'AB'
# MAGIC         WHEN codProducto LIKE '%AY%' THEN 'AY'
# MAGIC         WHEN codProducto LIKE '%CA%' THEN 'CA'
# MAGIC         WHEN codProducto LIKE '%EU%' THEN 'EU'
# MAGIC         WHEN codProducto LIKE '%EX%' THEN 'EX'
# MAGIC         WHEN codProducto LIKE '%PO%' THEN 'PO'
# MAGIC         WHEN codProducto LIKE '%GI%' THEN 'GI'
# MAGIC         WHEN codProducto LIKE '%LO%' THEN 'LO'
# MAGIC         WHEN codProducto LIKE '%ST%' THEN 'ST'
# MAGIC         WHEN codProducto LIKE '%VL%' THEN 'VL'
# MAGIC         WHEN codProducto LIKE '%RI%' THEN 'RI'
# MAGIC         ELSE 'n/a' 
# MAGIC     END AS codigoEntidadLegal
# MAGIC FROM gold_lakehouse.dim_producto
# MAGIC WHERE codProducto IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_entidad_legal_view AS
# MAGIC SELECT DISTINCT
# MAGIC     TRIM(centro) AS nombreInstitucion
# MAGIC FROM silver_lakehouse.sales
# MAGIC WHERE centro IS NOT NULL AND centro <> '';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_entidad_legal_view
