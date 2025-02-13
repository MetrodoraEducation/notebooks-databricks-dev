# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW institucion_sales_view AS 
# MAGIC SELECT 
# MAGIC     DISTINCT 
# MAGIC     institucion AS nombre_institucion
# MAGIC FROM 
# MAGIC     silver_lakehouse.sales;
# MAGIC
# MAGIC select * from institucion_sales_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_institucion
# MAGIC USING institucion_sales_view 
# MAGIC ON gold_lakehouse.dim_institucion.nombre_institucion = institucion_sales_view.nombre_institucion
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_institucion.nombre_institucion)
# MAGIC VALUES (institucion_sales_view.nombre_institucion)

# COMMAND ----------

# DBTITLE 1,dim_producto
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW entidad_legal_view AS
# MAGIC SELECT DISTINCT
# MAGIC                CASE 
# MAGIC                     WHEN entidadlegal IS NOT NULL AND entidadlegal != '' THEN TRIM(UPPER(entidadlegal))
# MAGIC                     ELSE 'n/a'
# MAGIC                     END nombre_institucion
# MAGIC FROM gold_lakehouse.dim_producto;
# MAGIC
# MAGIC select * from entidad_legal_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_institucion AS target
# MAGIC USING entidad_legal_view AS source
# MAGIC ON TRIM(UPPER(target.nombre_institucion)) = TRIM(UPPER(source.nombre_institucion))
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (nombre_institucion)
# MAGIC   VALUES (source.nombre_institucion);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_institucion
