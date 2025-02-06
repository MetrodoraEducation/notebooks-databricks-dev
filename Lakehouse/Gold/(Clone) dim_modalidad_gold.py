# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW modalidad_sales_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT 
# MAGIC     (SELECT IFNULL(MAX(modalidad_norm), 'n/a') FROM gold_lakehouse.mapeo_modalidad WHERE gold_lakehouse.mapeo_modalidad.modalidad = silver_lakehouse.sales.modalidad) AS nombre_modalidad,
# MAGIC     case when nombre_modalidad ='ON PREMISE' then 'P' 
# MAGIC     when nombre_modalidad ='ONLINE' then 'O' 
# MAGIC     when nombre_modalidad ='BLENDED' then 'B'
# MAGIC         when nombre_modalidad ='STREAMING' then 'S' 
# MAGIC     when nombre_modalidad ='ONLINE+PRACTICAS' then 'J'
# MAGIC         when nombre_modalidad ='ONLINE+CLASES' then 'C' 
# MAGIC     when nombre_modalidad ='SEMIPRESENCIAL' then 'M' 
# MAGIC     else 'n/a' end AS codigo
# MAGIC FROM 
# MAGIC     silver_lakehouse.sales;
# MAGIC
# MAGIC select * from modalidad_sales_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_modalidad
# MAGIC USING modalidad_sales_view 
# MAGIC ON gold_lakehouse.dim_modalidad.nombre_modalidad = modalidad_sales_view.nombre_modalidad
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_modalidad.codigo = modalidad_sales_view.codigo
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_modalidad.nombre_modalidad, gold_lakehouse.dim_modalidad.codigo)
# MAGIC VALUES (modalidad_sales_view.nombre_modalidad, modalidad_sales_view.codigo)
