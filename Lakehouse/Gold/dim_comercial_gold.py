# Databricks notebook source
# MAGIC %md
# MAGIC **Gestion sales**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW comercial_sales_view
# MAGIC     AS SELECT 
# MAGIC             distinct propietario_lead as nombre_comercial
# MAGIC                     ,institucion as equipo_comercial 
# MAGIC          FROM silver_lakehouse.sales where propietario_lead<>'n/a'

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from comercial_sales_view 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_comercial
# MAGIC USING comercial_sales_view 
# MAGIC ON gold_lakehouse.dim_comercial.nombre_comercial = comercial_sales_view.nombre_comercial
# MAGIC WHEN MATCHED THEN UPDATE SET gold_lakehouse.dim_comercial.equipo_comercial = comercial_sales_view.equipo_comercial
# MAGIC WHEN NOT MATCHED THEN INSERT (gold_lakehouse.dim_comercial.nombre_comercial, gold_lakehouse.dim_comercial.equipo_comercial, gold_lakehouse.dim_comercial.activo, gold_lakehouse.dim_comercial.fecha_desde)
# MAGIC VALUES (comercial_sales_view.nombre_comercial, comercial_sales_view.equipo_comercial, 1, current_date())
# MAGIC

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
