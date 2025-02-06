# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_COMERCIAL**

# COMMAND ----------

# MAGIC %md
# MAGIC **Gestion sales**

# COMMAND ----------

# DBTITLE 1,Cruze por name
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW comercial_sales_view AS 
# MAGIC SELECT DISTINCT 
# MAGIC     TRIM(sales.propietario_lead) AS nombre_comercial,
# MAGIC     TRIM(sales.institucion) AS equipo_comercial,
# MAGIC     COALESCE(lead.id, NULL) AS cod_comercial,
# MAGIC     CURRENT_DATE() AS fecha_Desde,
# MAGIC     NULL AS fecha_Hasta,
# MAGIC     1 AS activo
# MAGIC FROM silver_lakehouse.sales sales
# MAGIC LEFT JOIN silver_lakehouse.zoholeads lead
# MAGIC     ON UPPER(TRIM(sales.propietario_lead)) = UPPER(TRIM(CONCAT(lead.first_name, ' ', lead.last_name)))
# MAGIC WHERE TRIM(sales.propietario_lead) IS NOT NULL
# MAGIC   AND TRIM(sales.propietario_lead) <> 'n/a';
# MAGIC
# MAGIC select * from comercial_sales_view 

# COMMAND ----------

# DBTITLE 1,Cruze por email
#%sql
#CREATE OR REPLACE TEMPORARY VIEW comercial_sales_view AS 
#SELECT DISTINCT 
#    TRIM(sales.propietario_lead) AS nombre_comercial,
#    TRIM(sales.institucion) AS equipo_comercial,
#    COALESCE(lead.id, NULL) AS cod_comercial,
#    CURRENT_DATE() AS fecha_Desde,
#    NULL AS fecha_Hasta,
#    1 AS activo
#FROM silver_lakehouse.sales sales
#LEFT JOIN silver_lakehouse.zoholeads lead
#    ON UPPER(TRIM(sales.email)) = UPPER(TRIM(lead.email))
#WHERE TRIM(sales.propietario_lead) IS NOT NULL
#  AND TRIM(sales.propietario_lead) <> 'n/a';
#
#select * from comercial_sales_view 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre_comercial, COUNT(*)
# MAGIC FROM comercial_sales_view
# MAGIC GROUP BY nombre_comercial
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_comercial AS target
# MAGIC USING comercial_sales_view AS source
# MAGIC ON UPPER(target.nombre_comercial) = UPPER(source.nombre_comercial)
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.equipo_comercial = source.equipo_comercial,
# MAGIC         target.cod_comercial = source.cod_comercial,
# MAGIC         target.fecha_hasta = CASE 
# MAGIC                                 WHEN target.equipo_comercial <> source.equipo_comercial 
# MAGIC                                   OR target.cod_comercial <> source.cod_comercial 
# MAGIC                                 THEN CURRENT_DATE() 
# MAGIC                                 ELSE target.fecha_hasta 
# MAGIC                              END
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_comercial, equipo_comercial, cod_comercial, activo, fecha_desde, fecha_hasta)
# MAGIC     VALUES (source.nombre_comercial, source.equipo_comercial, source.cod_comercial, source.activo, source.fecha_desde, NULL);

# COMMAND ----------

# MAGIC %md
# MAGIC **Gestion Calls**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW comercial_calls_view
# MAGIC     AS SELECT distinct user_name from silver_lakehouse.aircallcalls where user_name<>'n/a'

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_comercial
# MAGIC USING comercial_calls_view 
# MAGIC ON gold_lakehouse.dim_comercial.nombre_comercial = comercial_calls_view.user_name
# MAGIC --WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_comercial.equipo_comercial = comercial_sales_view.equipo_comercial
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_comercial.nombre_comercial, gold_lakehouse.dim_comercial.equipo_comercial, gold_lakehouse.dim_comercial.activo, gold_lakehouse.dim_comercial.fecha_desde)
# MAGIC VALUES (comercial_calls_view.user_name, 'n/a', 1, current_date())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lakehouse.dim_comercial
